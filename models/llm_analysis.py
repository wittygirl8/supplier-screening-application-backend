import re
import os
import openai
from openai import AzureOpenAI
from dotenv import load_dotenv
import ast
import spacy
from collections import Counter, defaultdict
from datetime import datetime, date
import calendar
nlp = spacy.load("en_core_web_sm")
import psycopg2
from schemas.logger import logger

load_dotenv()

azure_endpoint = os.getenv('AZURE_ENDPOINT')
api_key = os.getenv('API_KEY')
CONFIG_TYPE = os.getenv('CONFIG')

# FOR LOCAL TESTING - CHANGE TO TRUE FOR FASTER PROMPT RESPONSE gpt-4-32k !!! ONLY IF REQUIRED
require_llm_response_speed = True
if require_llm_response_speed or (CONFIG_TYPE.lower() == "demo"):
    model_deployment_name = "ens-dev-gpt-4-32k"
else:
    model_deployment_name = "gpt-4o"


# OpenAI
client = AzureOpenAI(
    azure_endpoint=azure_endpoint,
    api_key=api_key,
    api_version="2024-07-01-preview"
)


def remove_first_and_last_two_sentences(paragraph):
    sentences = re.split(r'(?<=[.!?]) +', paragraph)
    if len(sentences) <= 4:
        return ""
    value = ' '.join(sentences[2:-2])
    return value


def summarize_text(title, text, person, flag):
    summary = None
    input_tokens = output_tokens = total_tokens= 0
    message_text = [
        {
            "role": "system",
            "content": (
                "You are an AI assistant tasked with summarizing articles. Follow these rules strictly:\n\n"
                "1. Summarize the article in 50 words or fewer, focusing on the reference or contribution of '{person}' if there is sufficient information about them in the text.\n"
                "2. If the text mentions '{person}' but does not provide enough detail about their role or contributions, return the exact Python string: 'N'.\n"
                "3. If the text does not mention '{person}' at all, return the exact Python string: 'N'.\n"
                "4. Provide no explanations, reasoning, or additional content outside of these rules."
            )
        },
        {
            "role": "user",
            "content": (
                f"Please summarize the following article for '{person}' in the text. "
                f"The input text is as follows: {text}"
            )
        }
    ]
    try:
        # Attempt to get the completion response
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Summary Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
        summary = completion.choices[0].message.content.strip()
        if summary == "'N'" or summary == 'N':
            logger.info(
                f"Summary Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
            return title, 201
        return summary, 200, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return title, 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens
    except Exception as e:
        logger.error(f"SUMMARY ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors

def related_to_person(person, text, flag):
    summary = None
    input_tokens = output_tokens = total_tokens = 0
    if flag == 'POI':
        message_text = [
            {
                "role": "system",
                "content":
                    (
                        "You are an entity recognition assistant. Determine if the word '{person}' is mentioned in the text, "
                        "allowing for minor spelling variations, super set of the name(Eg: Nithin Kamath in input = Nithin M Kamath in the article), and ignoring prefixes or suffixes (e.g., 'Mr.', 'Jr.'). "
                        "Treat case differences, spelling errors(Eg: Sundar Pichai= Sundar Picchai or Sundr Picchai), and longer text strings as valid mentions if they clearly reference the person. "
                        "Respond with 'Y' if mentioned, 'N' if not. Only return 'Y' or 'N'."
                    )
            },
            {"role": "user",
             "content": (
                 f"Text to analyze: {text}\n\n"
                 f"Question: Does the text directly mention '{person}'?"
             )
             }
        ]
    else:
        entity = person
        message_text = [
            {
                "role": "system",
                "content": (
                    "You are an entity recognition assistant. Your job is to determine if the core entity name '{entity}' is directly mentioned in the provided text. "
                    "Ignore all legal entity designations such as GMBH, Pvt. Ltd, LLP, Inc., LLC, or similar terms in both the input entity and the text. "
                    "For example, treat 'EY LLP', 'EY GMBH', and 'EY' as the same entity. "
                    "Also, treat abbreviations and expansions of the entity name as equivalent (e.g., 'EY' = 'Ernst and Young'). "
                    "Spelling variations (e.g., 'Earnst and Young') and case differences should also be considered valid matches. "
                    "Respond with 'Y' if the text clearly mentions the entity name (even with variations) and 'N' otherwise. "
                    "Provide no explanation or additional content; only respond with 'Y' or 'N'."
                )
            },
            {
                "role": "user",
                "content": (
                    f"Text to analyze: {text}\n\n"
                    f"Question: Does the text mention the core entity of '{entity}'?"
                )
            }
        ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Related to Entity Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
        summary = completion.choices[0].message.content.strip()
        match = re.search(r"\b(Y|N)\b", summary.upper())

        return match.group(0).lower(), 200, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return '', 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"RELATED TO PERSON ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def related_to_company(company, text, flag):
    summary = None
    input_tokens = output_tokens = total_tokens = 0
    message_text = [
        {
            "role": "system",
            "content": (
                "You are an entity recognition assistant. Your job is to determine if the core entity name '{entity}' is directly mentioned in the provided text. "
                "Ignore all legal entity designations such as GMBH, Pvt. Ltd, LLP, Inc., LLC, or similar terms in both the input entity and the text. "
                "For example, treat 'EY LLP', 'EY GMBH', and 'EY' as the same entity. "
                "Also, treat abbreviations and expansions of the entity name as equivalent (e.g., 'EY' = 'Ernst and Young'). "
                "Spelling variations (e.g., 'Earnst and Young') and case differences should also be considered valid matches. "
                "Respond with 'Y' if the text clearly mentions the entity name (even with variations) and 'N' otherwise. "
                "Provide no explanation or additional content; only respond with 'Y' or 'N'."
            )
        },
        {
            "role": "user",
            "content": (
                f"Text to analyze: {text}\n\n"
                f"Question: Does the text mention the core entity of '{company}'?"
            )
        }
    ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Related to Entity Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")

        summary = completion.choices[0].message.content.strip()
        match = re.search(r"\b(Y|N)\b", summary.upper())
        return match.group(0).lower(), 200, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return '', 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            # total_tokens = input_tokens
            # logger.info(
            #     f"Related to Entity Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"RELATED TO COMPANY ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def related_to_domain(domain, text, flag):
    response = None
    input_tokens = output_tokens = total_tokens = 0
    # Define the system prompt with the new logic
    message_text = [
        {"role": "system",
         "content": "You are a domain relevance evaluator. Your task is to determine whether the provided text is clearly relevant to the given domain. Evaluate if the text directly or indirectly relates to the domain. If it does, respond with 'Y'. If it does not, respond with 'N'."},
        {"role": "user",
         "content": f"Please evaluate if the following text is relevant to the domain: {domain}. If the text is related, respond with 'Y'. If the text is unrelated, respond with 'N'."},
        {"role": "user", "content": f"Text to evaluate: {text}."},
        {"role": "system", "content": "Consider the following:\n"
                                      "- If the domain is 'Technology' and the text mentions related terms like 'smartphones', 'software', etc., respond with 'Y'.\n"
                                      "- If the domain is 'Healthcare' and the text discusses related topics like 'medicine', 'health', 'fitness', etc., respond with 'Y'.\n"
                                      "- If the domain is 'Finance' and the text covers terms like 'stocks', 'investment', 'economy', etc., respond with 'Y'.\n"
                                      "- If the text talks about topics that are entirely unrelated to the domain, such as 'fashion', 'politics', 'business' etc when the domain is 'Technology', respond with 'N'.\n"
                                      "- If the text mentions the specific domain keyword (e.g., 'Technology', 'Healthcare', 'Finance') directly, respond with 'Y'.\n"
                                      "- Only respond 'Y' if the connection to the domain is clear and directly relevant. If there is any doubt, respond 'N'."}
    ]
    try:
        # Check if the domain word appears in the text
        if re.search(r"\b" + re.escape(domain) + r"\b", text, re.IGNORECASE):
            return 'Y', 200, total_tokens

        # Proceed with the original completion if no direct match is found
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text,
            temperature=0
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Domain Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
        response = completion.choices[0].message.content.strip()
        match = re.search(r"\b(Y|N)\b", response.upper())
        if match:
            return match.group(0), 200, total_tokens
        else:
            return "N", 202, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return 'N', 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"RELATED TO DOMAIN ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def sentiment(text, person, flag):
    response = None
    input_tokens = output_tokens = total_tokens = 0
    message_text = [
        {
            "role": "system",
            "content":
                (
                    "You are a sentiment analyst that evaluates the sentiment of news articles. \n"
                    "It can be can only be categorised as either strongly positive or strongly negative. Anything else is neutral. \n"
                    "The output from the prompt should be one of the following Python strings: 'positive', 'negative' or 'neutral'.\n"
                    "Examples of actions that are considered strongly negative include: bribe, forgery, legal issues, fraud, corruption, scandal, accusations of misconduct, unethical behavior.\n"
                    "Examples of actions that are considered strongly positive include: gaining funding, receiving awards, achieving success, positive business growth, community contributions, endorsements, philanthropic actions.\n"
                    "If the person being analyzed is passive or indirectly referenced in a positive light (e.g., through philanthropy or past achievements), categorize as neutral unless the action itself is clearly positive.\n"
                    "Anything else, including passive involvement or neutral actions, should be categorized as neutral.\n"
                )
        },
        {
            "role": "user",
            "content": (
                f"Consider the following \n"
                f"Analyze the sentiment strictly based on the {person}'s action in the news article.\n"
                f"If the {person} is mentioned only passively or in comparison, categorize the sentiment as neutral.\n"
                f"If no clear action is found, analyze based on the {person}'s passive involvement or relation in the article.\n"
                f"Otherwise, analyze the {person}'s image portrayed in the article.\n "
                f"The sentiment of the input text :{text} \n"
            )
        }
    ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Sentiment Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")

        response = completion.choices[0].message.content.strip()

        sentiment_match = re.search(r"\b(positive|negative|neutral)\b", response.lower())

        if sentiment_match:
            return sentiment_match.group(0).lower(), 200, total_tokens
        else:
            return "uncategorised", 404, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            error_message = e.message
            if 'hate' in error_message or 'self_harm' in error_message or 'sexual' in error_message or 'violence' in error_message:
                return 'negative', 201, total_tokens

            elif 'jailbreak' in error_message:
                return 'uncategorised', 404, total_tokens

        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"SENTIMENT ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def keyword(text, flag):
    response = None
    input_tokens = output_tokens = total_tokens = 0
    message_text = [
        {"role": "system",
         "content": "You are now a language assistant that provides keywords in the form of a python list."},
        {"role": "user",
         "content": f"For the provided news article, generate a list of 10 categorical keywords in the order of relevancy. The input text is: {text}"}
    ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        if completion.choices[0].message.content:
            input_tokens = completion.usage.prompt_tokens
            output_tokens = completion.usage.completion_tokens
            total_tokens = completion.usage.total_tokens
            logger.info(
                f"Sentiment Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")

            response = completion.choices[0].message.content.strip()
        else:
            response = []
        try:
            keywords = ast.literal_eval(response)
            if isinstance(keywords, list):
                return keywords, 200, total_tokens
            else:
                return [], 202, total_tokens  # List Parsing
        except (ValueError, SyntaxError):
            return [], 202, total_tokens  # List Parsing

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return [], 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"KEYWORD ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def keyword_categorisation(article: str, keywords: list) -> tuple:
    """
    Function to take the article content and perform NER
    - then match it to the extracted keywords to categorise each keyword as one of: "Entity" ; "POI" ; "General-Keyword"
    - also in this function perform filtering to remove types of keywords from the prompt which arent required e.g. Dates
    :param article: article content -- is this summary (?)
    :param keywords: list of keywords
    :return: keywords - filtered down list of keywords; keywords_categorised - list of dicts of each keyword and its category
    """

    article_nlp = nlp(article)

    # For now: we can allow "PRODUCT" as an Entity as it might have useful results?
    # PERSON / ORG, PRODUCT
    entities = []
    keywords_in_article_nlp = []
    for ent in article_nlp.ents:
        # print(f"Entity: {ent.text}, Label: {ent.label_}")
        # entity_words = set(ent.text.lower().split())
        if ent.text.lower() in [kw.lower() for kw in keywords]:
            # print(ent.text, ent.label_)

            keywords_in_article_nlp.append(ent.text)

            # removing unwanted ner labels here
            if ent.label_ in ["DATE", "TIME", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL"]:
                continue

            if ent.label_ in ["ORG", "PRODUCT"]:
                ent_label = "Entity"
            elif ent.label_ in ["PERSON"]:
                ent_label = "POI"
            else:
                ent_label = "General-Keyword"

            entities.append({
                "keyword": ent.text, "keyword-type": ent_label
            })
        # else: #other approach to use sub-words and matching any - can add this if needed but might be false result
        #     for kw in keywords:
        #         if entity_words & set(kw.lower().split()):
        #             entities.append({
        #                 "keyword": ent.text, "keyword-type": ent.label_
        #             })

    # Handling keywords not found directly in the text...
    # Enable further categorisation if needed after further testing
    for kw in keywords:
        if not (kw in keywords_in_article_nlp):

            entities.append({
                "keyword": kw, "keyword-type": "General-Keyword"
            })

            # # another layer of checks for keywords which may not be directly in the article content --Enable if needed
            # kw_nlp = nlp(kw)
            # for ent in kw_nlp.ents:
            #     if ent.text == kw:  # Only consider whole cases??
            #         print("HERE.....")
            #         print(ent.text, ent.label_)
            #
            #         # removing unwanted ner labels here
            #         if ent.label_ in ["DATE", "TIME", "PERCENT", "MONEY", "QUANTITY", "ORDINAL", "CARDINAL"]:
            #             continue
            #
            #         if ent.label_ in ["ORG", "PRODUCT"]:
            #             ent_label = "Entity"
            #         elif ent.label_ in ["POI"]:
            #             ent_label = "POI"
            #         else:
            #             ent_label = "General-Keyword"
            #
            #         entities.append({
            #             "keyword": ent.text, "keyword-type": ent_label,
            #             "ner-type": ent.label_
            #         })

    # Remove duplicates - TODO think about how to remove other cases like sub-phrases with different categorisation
    article_keywords_categorised = []
    seen = set()
    for d in entities:
        dict_tuple = tuple(sorted(d.items()))
        if dict_tuple not in seen:
            article_keywords_categorised.append(d)
            seen.add(dict_tuple)

    article_keywords = list(set([w["keyword"] for w in article_keywords_categorised]))

    return article_keywords_categorised, article_keywords

    # REFERENCE LIST OF ent.label_
    # PERSON:      People, including fictional.
    # NORP:        Nationalities or religious or political groups.
    # FAC:         Buildings, airports, highways, bridges, etc.
    # ORG:         Companies, agencies, institutions, etc.
    # GPE:         Countries, cities, states.
    # LOC:         Non-GPE locations, mountain ranges, bodies of water.
    # PRODUCT:     Objects, vehicles, foods, etc. (Not services.)
    # EVENT:       Named hurricanes, battles, wars, sports events, etc.
    # WORK_OF_ART: Titles of books, songs, etc.
    # LAW:         Named documents made into laws.
    # LANGUAGE:    Any named language.
    # DATE:        Absolute or relative dates or periods.
    # TIME:        Times smaller than a day.
    # PERCENT:     Percentage, including ”%“.
    # MONEY:       Monetary values, including unit.
    # QUANTITY:    Measurements, as of weight or distance.
    # ORDINAL:     “first”, “second”, etc.
    # CARDINAL:    Numerals that do not fall under another type.


async def keyword_aggregation(news: list, name: str, company: str) -> list:
    """
    Perform keyword aggregation for all the articles in the query combined
    and return the result with the count and the "searchable" parameter
    :param news: final news articles with keywords categorised
    :return: aggregated list of keywords with count
    """
    TOPN_LIMIT_LIST = 10
    keyword_token =0

    def _scale_count(count):
        if max_count == min_count:
            return 1, keyword_token
        return round(1 + 4 * (count - min_count) / (max_count - min_count)), keyword_token

    all_items = []
    for article in news:
        categorised_keywords = article.get("keywords_categorised", [])
        for kw in categorised_keywords:
            all_items.append(kw)

    all_keywords = []
    for it in all_items:

        if it['keyword'].lower() == company.lower():
            it['keyword-type'] = "Entity"

        if it['keyword'].lower() == name.lower():
            all_items.remove(it)
        else:
            all_keywords.append(it['keyword'])

    # multiple occurrences handler
    for keyw in list(set(all_keywords)):
        selected_it = [item['keyword-type'] for item in all_items if item['keyword'] == keyw]

        # if a keyword is subpart part of another keyword which is a POI aka surname, ignore it
        # if it is a POI and it is not a full name, ignore it

        if ('POI' in selected_it) and (('Entity' in selected_it) or ('General-Keyword' in selected_it)) and (len(keyw.split())==1):
            keyw_ent_type = "Entity"
        if ('POI' in selected_it) and (len(keyw.split())==1):
            keyw_ent_type = 'General-Keyword'
        elif 'POI' in selected_it:
            keyw_ent_type = "POI"
        elif 'Entity' in selected_it:
            keyw_ent_type = "Entity"
        else:
            keyw_ent_type = 'General-Keyword'

        for item in all_items:
            if item['keyword'] == keyw:
                item['keyword-type'] = keyw_ent_type

    data_tuples = [tuple(d.items()) for d in all_items]
    counter = Counter(data_tuples)
    counts = [count for _, count in counter.items()]
    aggregated_data = []
    if len(counts) > 0:
        min_count = min(counts)
        max_count = max(counts)

        aggregated_data = [
            {**dict(items), 'count': count, 'sizing_score': _scale_count(count),
            'searchable': not dict(items).get("keyword-type") == "General-Keyword"}
            for items, count in counter.items()
        ]

        # aggregated_data = sorted(aggregated_data, key=lambda x: x['count'], reverse=True)[:TOPN_LIMIT_LIST]
        aggregated_data = sorted(aggregated_data,
                                 key=lambda x: (-x['count'], {'POI': 0, 'Entity': 1, 'General-Keyword': 2}.get(x['keyword-type'], 3)))[:TOPN_LIMIT_LIST]

        for item in aggregated_data:
            keyw = item['keyword']
            if keyw.lower() == company.lower():
                item['keyword-type'] = "Entity"  # recat over here again
            else:
                related_articles = [item['summary'] for item in news if keyw in item['keywords']]
                related_article = related_articles[0]
                if len(related_article) < 5:
                    related_article = ""
                verification, code, keyword_token = keyword_verification(keyw, related_article)
                if code == 200:
                    if verification != item['keyword-type']:
                        # print(f"Changing {keyw} from {item['keyword-type']} to {verification}")
                        item['keyword-type'] = verification
                else:
                    aggregated_data.remove(item)

        # recat searchable
        for it in aggregated_data:
            if it["keyword-type"] == "General-Keyword":
                it["searchable"] = False
            else:
                it["searchable"] = True

        # remove dupes again??? - inefficient change later
        seen = set()
        unique_data = []
        for d in aggregated_data:
            key = (d['keyword'])
            if key not in seen:
                unique_data.append(d)
                seen.add(key)

        aggregated_data = unique_data

    return aggregated_data, keyword_token


async def sentiment_aggregation(news: list, plot:bool) -> list:
    """
    TODO Make this more efficient and test for multiple years case
    :param news: list of dicts; each dict is an article which has "sentiment" attribute
    :return: aggregated count of articles of each sentiment type by period (month for smaller timeframes, quarter or year for larger time frames)
    """
    def get_period_key(date_str, period_type):
        date = datetime.strptime(date_str, "%Y-%m-%d")

        if period_type == 'year':
            return date.year
        elif period_type == 'quarter':
            return f"Q{(date.month - 1) // 3 + 1} {date.year}"
        else:  # 'month'
            return date.strftime('%b %Y')  # Format as 'Month YYYY'

    date_range = [datetime.strptime(article['date'], "%Y-%m-%d") for article in news]
    min_date = min(date_range)
    max_date = max(date_range)
    max_date = max_date.replace(day=calendar.monthrange(max_date.year, max_date.month)[1])

    time_span = max_date - min_date
    if time_span.days > 365 * 4:
        aggregation_period = 'year'
    elif time_span.days > 30 * 16:
        aggregation_period = 'quarter'
    else:
        aggregation_period = 'month'

    aggregated_sentiments = defaultdict(lambda: {'neutral': 0, 'positive': 0, 'negative': 0})

    # Aggregate the sentiments
    for article in news:
        period_key = get_period_key(article['date'], aggregation_period)
        period_key = str(period_key)
        sentiment = article['sentiment']

        if sentiment == 'positive':
            aggregated_sentiments[period_key]['positive'] += 1
        elif sentiment == 'neutral':
            aggregated_sentiments[period_key]['neutral'] += 1
        elif sentiment == 'negative':
            aggregated_sentiments[period_key]['negative'] += 1

    all_periods = []
    if aggregation_period == 'year':
        for year in range(min_date.year, max_date.year + 1):
            all_periods.append(str(year))
    elif aggregation_period == 'quarter':
        for year in range(min_date.year, max_date.year + 1):
            for quarter in range(1, 5):
                all_periods.append(f"Q{quarter} {year}")
    else:  # 'month'
        current_date = min_date
        while current_date <= max_date:
            all_periods.append(current_date.strftime('%b %Y'))
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)

    aggregated_sentiment_counts = []
    for period in all_periods:
        if period not in aggregated_sentiments:
            aggregated_sentiments[period] = {'neutral': 0, 'positive': 0, 'negative': 0}
        if plot==False:
            result = {
                "month": period,
                "neutral": aggregated_sentiments[period]['neutral'],
                "positive": aggregated_sentiments[period]['positive'],
                "negative": aggregated_sentiments[period]['negative']
            }
        else:
            result = {
                "month": period,
                "negative": aggregated_sentiments[period]['negative']
            }
        aggregated_sentiment_counts.append(result)

    return aggregated_sentiment_counts

def link_sorting_and_demo_reordering(news: list, num_articles_per_month: int = 3, demo_flag: bool = False) -> list:
    """
    Sorts the news links extracted to reorder them for demo purpose
    Takes num_articles_per_month for each month and adding them at the start of the list
    If not demo_flag, just sorts by take and returns
    Assumes article dates in YYYY-MM-DD
    :param demo_flag: bool True if needing the demo reordering, else simple date sorting
    :param news: list of dicts
    :param num_articles_per_month: default 3
    :return: same news, just resorted
    """
    if demo_flag:
        grouped_by_month = defaultdict(list) #group by each month
        for article in news:
            date_str = article.get('date')
            if date_str:
                date = datetime.strptime(date_str, '%Y-%m-%d')
                month_key = date.strftime('%Y-%m')
                grouped_by_month[month_key].append(article)

        reordered_articles = []

        for month in sorted(grouped_by_month.keys(), reverse=True):
            selected_articles = grouped_by_month[month][:num_articles_per_month]
            reordered_articles.extend(selected_articles)
        for month in sorted(grouped_by_month.keys(), reverse=True): # adding back the rest
            remaining_articles = grouped_by_month[month][num_articles_per_month:]
            reordered_articles.extend(remaining_articles)
    else:
        reordered_articles = sorted(news, key=lambda x: datetime.strptime(x.get('date'), '%Y-%m-%d'), reverse=True) # simple resorting

    return reordered_articles



def keyword_verification(keyword, related_article):
    response = None
    input_tokens = output_tokens = total_tokens = 0
    if related_article == "":
        question = f"Does the following word '{keyword}' refer to a specific Person's name or Company's name?"
    else:
        question = f"Given the following context: '{related_article}', \n does the following word '{keyword}' refer to a specific Person's name or Company's name?"

    content_base = "\nIf it is not a specific Person's name or a Company name, reply 'Other'. It must be a name, not a designation or generic description. Answer only one of the options: 'Person' or 'Company' or 'Other'."
    content = f"{question} {content_base}"

    message_text = [
        {"role": "system",
         "content": "You are now a language assistant that categorises proper nouns as People or Companies based on a given context."},
        {"role": "user",
         "content": content}
    ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Sentiment Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")

        if completion.choices[0].message.content:
            response = completion.choices[0].message.content.strip()

            category_person_match = re.search(r"\b(person)\b", response.lower())
            category_company_match = re.search(r"\b(company)\b", response.lower())
            category_other_match = re.search(r"\b(other)\b", response.lower())

            if category_person_match:
                return "POI", 200, total_tokens
            elif category_company_match:
                return "Entity", 200, total_tokens
            elif category_other_match:
                return "General-Keyword", 200, total_tokens
            else:
                return "", 404, total_tokens
        else:
            return "", 404, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return "", 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"KEYWORD VERIFICATION ERROR RECEIVED --------> {str(e)}")
        return None, 404, total_tokens    # for other errors


def cross_verifying_kpi(text, person, topic):
    response = None
    input_tokens = output_tokens = total_tokens = 0
    message_text = [
        {"role": "system", "content": """You are now a fact verifier. Verify if the person mentioned is involved in a topic in a given article.
                                        The output should be a python string. 'Y' if true or 'N' if false. No other output expected
                                        """},
        {"role": "user",
         "content": f"For the provided news article, Verify if the {person} in involved in {topic}. The input text is: {text}"}
    ]
    try:
        completion = client.chat.completions.create(
            model=model_deployment_name,
            messages=message_text
        )
        input_tokens = completion.usage.prompt_tokens
        output_tokens = completion.usage.completion_tokens
        total_tokens = completion.usage.total_tokens
        logger.info(
            f"Cross Verification Prompt ----> Input Tokens: {input_tokens}, Output Tokens: {output_tokens}, Total Tokens: {total_tokens}")
        summary = completion.choices[0].message.content.strip()
        # print(summary)
        match = re.search(r"\b(Y|N)\b", summary.upper())

        return match.group(0).lower(), 200, total_tokens

    except openai.BadRequestError as e:
        if e.code == 'content_filter':
            return '', 201, total_tokens
        return None, 404, total_tokens

    except openai.RateLimitError as e:
        if e.code == '429':
            return None, 429, total_tokens
        return None, 404, total_tokens

    except Exception as e:
        logger.error(f"CROSS VERIFICATION ERROR RECEIVED --------> {e}")
        return None, 404, total_tokens


def categorize_news(article: str):
    # Adverse Media - Business Ethics / Reputational Risk / Code of Conduct (AMR)
    AMR = [
        "Bankruptcy", "Insolvency", "Financial Ruin", "Liquidation",
        "Business Crimes", "Corporate Fraud", "White Collar Crime", "Financial Misconduct",
        "Copyright Infringement", "Intellectual Property Theft", "Piracy", "Plagiarism",
        "Data Privacy and Protection", "Information Security", "Cybersecurity", "Data Breach",
        "Hate Groups, Hate Crimes", "Racial Crimes", "Bias Crimes", "Discrimination Offenses",
        "Prostitution", "Sex Work", "Solicitation", "Pandering",
        "Legal Marijuana Dispensaries", "Cannabis Shops", "Weed Dispensaries", "Marijuana Retailers",
        "Loan Sharking", "Usury", "Illegal Lending", "Predatory Lending",
        "Misconduct", "Impropriety", "Wrongdoing", "Unethical Behavior",
        "Virtual Currency", "Cryptocurrency", "Bitcoin", "Digital Assets"
    ]

    # Adverse Media - Other Criminal Activity (AMO)
    AMO = [
        "Abuse", "Maltreatment", "Mistreatment", "Exploitation",
        "Arson", "Fire-Setting", "Incendiary Crime", "Pyromania",
        "Assault, Battery", "Physical Attack", "Violent Crime", "Aggravated Assault",
        "Burglary", "Breaking and Entering", "Housebreaking", "Home Invasion",
        "Cybercrime", "Online Fraud", "Hacking", "Digital Crime",
        "Drug Possession", "Controlled Substance Possession", "Illegal Drug Holding",
        "Drug Trafficking", "Narcotics Trade", "Drug Smuggling", "Drug Dealing",
        "Environmental Crimes", "Ecological Offenses", "Pollution Violations", "Wildlife Crime",
        "Fugitive", "Escapee", "Runaway", "Wanted Person",
        "Gambling", "Betting", "Wagering", "Gaming Offenses",
        "Human Rights", "Civil Liberties", "Fundamental Rights", "Social Justice",
        "Weapons Possession", "Illegal Firearms", "Gun Possession", "Armed Offense",
        "Identity Theft", "Impersonation", "Personal Data Fraud", "ID Fraud",
        "Kidnapping", "Abduction", "Hostage-Taking", "Unlawful Imprisonment",
        "Murder", "Homicide", "Manslaughter", "Killing",
        "Nonspecific Crimes", "General Offenses", "Unclassified Crimes",
        "Obscenity", "Indecency", "Lewd Conduct", "Pornographic Violations",
        "Organized Crime", "Mafia", "Gang Activity", "Criminal Syndicate",
        "Perjury", "False Testimony", "Lying Under Oath", "Witness Tampering",
        "Possession of Stolen Property", "Receiving Stolen Goods", "Handling Stolen Property",
        "Robbery", "Theft with Violence", "Armed Robbery", "Mugging",
        "Sex Offences", "Sexual Assault", "Rape", "Indecent Exposure",
        "Smuggling", "Contraband Trafficking", "Illegal Importation", "Bootlegging",
        "Spying", "Espionage", "Covert Surveillance", "Intelligence Leaks",
        "Terrorism", "Extremism", "Radicalism", "Political Violence",
        "Theft", "Larceny", "Stealing", "Shoplifting",
        "Human Trafficking", "Modern Slavery", "Forced Labor", "Sex Trafficking"
    ]

    # Bribery / Corruption / Fraud (BCF)
    BCF = [
        "Bribery", "Corruption", "Kickbacks", "Illicit Payments",
        "Counterfeiting", "Forgery", "Fake Goods", "Imitation Fraud",
        "Conspiracy", "Collusion", "Criminal Plot", "Illegal Agreement",
        "Fraud", "Deception", "Scam", "Financial Crime",
        "Money Laundering", "Illicit Funds", "Financial Smuggling", "Asset Concealment",
        "Mortgage Wrongdoing", "Mortgage Fraud", "Home Loan Fraud",
        "Money Services Business", "Financial Institutions", "Remittance Services",
        "Real Estate Actions", "Property Fraud", "Land Deals",
        "Tax-Related", "Tax Evasion", "Tax Fraud", "Financial Noncompliance", "Scandal"
    ]

    # Regulatory (REG)
    REG = [
        "Denied Entity", "Restricted Individuals", "Prohibited Organizations",
        "Foreign Agent Registration Act", "FARA Violations", "Foreign Lobbying Offenses",
        "Forfeiture", "Asset Seizure", "Confiscation", "Property Loss",
        "Regulatory Action", "Compliance Violations", "Legal Sanctions",
        "Securities Violations", "Stock Market Fraud", "Insider Trading",
        "Watch List", "Restricted List", "Blacklisted Entities"
    ]

    # Sanctions (SAN)
    SAN = [
        "Former OFAC", "Sanctions List Removal", "Past Restrictions",
        "Former Sanctions", "Lifted Embargoes", "Past Trade Bans",
        "Iran Connect", "Iran-Related Transactions", "Sanctions Compliance",
        "Sanctions Connect", "Sanctions Monitoring", "Trade Restrictions", "Sanctions", "Sanction"
    ]

    # Politically Exposed Persons (PEP)
    PEP = [
        "Politically Exposed Persons", "PEPs", "High-Risk Officials"
    ]

    # categories = {
    #     "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct": AMR,
    #     "Adverse Media - Other Criminal Activity": AMO,
    #     "Bribery / Corruption / Fraud": BCF,
    #     "Regulatory": REG,
    #     "Sanctions": SAN,
    #     "Politically Exposed Persons": PEP
    # }

    categories = {
        "Adverse Media - Business Ethics / Reputational Risk / Code of Conduct": AMR,
        "Adverse Media - Other Criminal Activity": AMO,
        "Bribery / Corruption / Fraud": BCF,
        "Regulatory": REG
    }

    article_lower = article.lower()
    matched_categories = {category: [] for category in categories}
    for category, keywords in categories.items():
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if re.search(r"\s*\b" + re.escape(keyword_lower) + r"\b\s*", article_lower):
                matched_categories[category].append(keyword)

    # Remove empty categories
    matched_categories = {k: v for k, v in matched_categories.items() if v}

    if not matched_categories:
        matched_categories["General"] = ["General"]

    return matched_categories

def get_db_connection():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )
    return conn


from datetime import datetime, date  # Correct import

from datetime import datetime, date
from psycopg2 import sql

def insert_article_into_db(all_articles, country, start_date, end_date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        for article_data in all_articles:
            insert_query = sql.SQL("""
                INSERT INTO public.news_master (
                    name, title, category, summary, news_date,
                    link, sentiment, content_filtered, country,
                    start_date, end_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (name, link, news_date) DO UPDATE 
                SET category = EXCLUDED.category, 
                    summary = EXCLUDED.summary, 
                    news_date = EXCLUDED.news_date, 
                    sentiment = EXCLUDED.sentiment, 
                    content_filtered = EXCLUDED.content_filtered,
                    start_date = EXCLUDED.start_date,
                    end_date = EXCLUDED.end_date;
            """)

            # Ensure date formatting
            def format_date(value):
                if isinstance(value, (date, datetime)):
                    return value.strftime('%Y-%m-%d')
                return value
            values = (
                article_data['name'],
                article_data['title'],
                article_data['category'],
                article_data['summary'],
                format_date(article_data['date']),
                article_data['link'],
                article_data['sentiment'],
                bool(article_data['content_filtered']),
                country,
                format_date(start_date),
                format_date(end_date),
            )
            # Debug: log values if needed
            query_with_values = insert_query.as_string(conn) % tuple(
                f"'{v}'" if isinstance(v, str) else str(v) for v in values
            )
            logger.debug(f"Executing query: {query_with_values}")

            cur.execute(insert_query, values)

        conn.commit()
        logger.info("Inserted successfully.")
    except Exception as e:
        logger.error(f"Error insert_article_into_db: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def insert_token_usage_into_db(payload, token_used, model):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        insert_query = sql.SQL("""
            INSERT INTO public.token_monitor (
                payload, token_used, openai_model
            )
            VALUES (%s, %s, %s);
        """)

        values = (
            payload,
            token_used,
            model
        )
        # Debug: log values if needed
        query_with_values = insert_query.as_string(conn) % tuple(
            f"'{v}'" if isinstance(v, str) else str(v) for v in values
        )
        logger.debug(f"Executing query: {query_with_values}")

        cur.execute(insert_query, values)

        conn.commit()
        logger.info("Inserted successfully.")
    except Exception as e:
        logger.error(f"Error insert_article_into_db: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

async def check_existing_articles_in_db_with_link(news: list, name: str) -> list:
    conn = get_db_connection()
    cur = conn.cursor()
    existing_articles = []

    try:
        for article in news:
            link = article['link']
            select_query = sql.SQL("""
                SELECT name, title, category, summary, news_date, link, sentiment, content_filtered FROM public.news_master 
                WHERE link = %s AND LOWER(name) = %s
            """)
            cur.execute(select_query, (link, name.lower()))
            result = cur.fetchone()

            if result:
                existing_articles.append({
                    'name': result[0],
                    'title': result[1],
                    'category': result[2],
                    'summary': result[3],
                    'date': result[4].strftime('%Y-%m-%d'),
                    'link': result[5],
                    'sentiment': result[6],
                    'content_filtered': result[7]
                })
    except Exception as e:
        logger.error(f"check_existing_articles_in_db_with_link: {e}")
    finally:
        cur.close()
        conn.close()

    return existing_articles

def check_existing_articles_in_db_for_daterange(name: str, start_date, end_date, country) -> list:
    conn = get_db_connection()
    cur = conn.cursor()
    existing_articles = []
    error = ('Error:429', 'Error:404', 'News link extraction:429')
    placeholders = sql.SQL(', ').join([sql.Placeholder() for _ in error])
    try:
        select_query = sql.SQL("""
                SELECT name, title, category, summary, news_date, link, sentiment, content_filtered, start_date, end_date
                FROM public.news_master 
                WHERE LOWER(name) = %s AND news_date BETWEEN %s AND %s AND LOWER(country) = %s AND summary NOT IN ({})
            """).format(placeholders)
        params = (name.lower(), start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), country.lower()) + error

        cur.execute(select_query, params)
        logger.debug("SQL Query: %s", select_query.as_string(conn))
        logger.debug("Query Parameters: %s", params)
        results = cur.fetchall()
        existing_articles = []
        if results:
            for result in results:
                existing_articles.append({
                    'name': result[0],
                    'title': result[1],
                    'category': result[2],
                    'summary': result[3],
                    'date': result[4].strftime('%Y-%m-%d'),
                    'link': result[5],
                    'sentiment': result[6],
                    'content_filtered': result[7],
                    'start_date': result[8],
                    'end_date': result[9]
                })
    except Exception as e:
        logger.error(f"Error check_existing_articles_in_db_for_daterange: {str(e)}")
    finally:
        cur.close()
        conn.close()

    return existing_articles

def check_existing_articles_in_db_with_name(name: str, country) -> list:
    conn = get_db_connection()
    cur = conn.cursor()
    existing_articles = []

    try:
        select_query = sql.SQL("""
            SELECT name, title, category, summary, news_date, link, sentiment, content_filtered, start_date, end_date
            FROM public.news_master 
            WHERE LOWER(name) = %s AND LOWER(country) = %s AND LOWER(sentiment) = 'negative'
        """)

        cur.execute(select_query, (name.lower(), country.lower()))
        query_with_values = select_query.as_string(conn) % (name, country)
        logger.debug(f"Executing query: {query_with_values}")

        results = cur.fetchall()
        if results:
            for result in results:
                existing_articles.append({
                    'name': result[0],
                    'title': result[1],
                    'category': result[2],
                    'summary': result[3],
                    'date': result[4].strftime('%Y-%m-%d'),
                    'link': result[5],
                    'sentiment': result[6],
                    'content_filtered': result[7],
                    'start_date': result[8],
                    'end_date': result[9]
                })
    except Exception as e:
        logger.error(f"Error check_existing_articles_in_db_with_name: {str(e)}")
    finally:
        cur.close()
        conn.close()

    return existing_articles


def delete_articles_by_name_daterange_country(name: str, start_date, end_date, country: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    deleted = False

    try:
        delete_query = sql.SQL("""
                DELETE FROM public.news_master
                WHERE LOWER(name) = %s AND news_date BETWEEN %s AND %s AND LOWER(country) = %s
            """)

        cur.execute(delete_query, (name.lower(), start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), country.lower()))
        conn.commit()  # Commit the transaction

        deleted = cur.rowcount > 0  # Check if any rows were deleted
        logger.info(f"Deleted {cur.rowcount} records from news_master.")

    except Exception as e:
        logger.error(f"Error delete_articles_by_name_daterange_country: {str(e)}")
        conn.rollback()  # Rollback in case of error
    finally:
        cur.close()
        conn.close()

    return deleted  # Returns True if deletion was successful, False otherwise


def delete_articles_by_name_daterange_country_error(name: str, start_date, end_date, country: str) -> bool:
    conn = get_db_connection()
    cur = conn.cursor()
    deleted = False

    try:
        delete_query = sql.SQL("""
                DELETE FROM public.news_master
                WHERE LOWER(name) = %s AND news_date BETWEEN %s AND %s AND LOWER(country) = %s AND sentiment = 'N/A'
            """)

        cur.execute(delete_query, (name.lower(), start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"), country.lower()))
        conn.commit()  # Commit the transaction

        deleted = cur.rowcount > 0  # Check if any rows were deleted
        logger.info(f"Deleted {cur.rowcount} records from news_master.")

    except Exception as e:
        logger.error(f"Error delete_articles_by_name_daterange_country: {str(e)}")
        conn.rollback()  # Rollback in case of error
    finally:
        cur.close()
        conn.close()

    return deleted



def heuristic_validation(article, name):
    cleaned_name = remove_company_suffix(name.lower())
    logger.info(f"name: {name}, cleaned_name: {cleaned_name}")
    if cleaned_name.lower() in article.lower() or name.lower() in article.lower():
        return True
    name_words = cleaned_name.split()

    # Create a regex pattern to find words in any order (but still consecutively)
    phrase_pattern = r"\b(?:\w+\s+){0," + str(len(name_words) - 1) + r"}" + r"\s*".join(
        re.escape(word) for word in name_words) + r"\b"

    # Check if the sequence of words exists in the article (consecutive but order not fixed)
    if re.search(phrase_pattern, article.lower()):
        return True
    return False


# def extract_context_around_mentions(article, name, window_size=100):
#     # Clean the name (remove suffixes, make lowercase)
#     cleaned_name = remove_company_suffix(name.lower())
#     logger.info(f"name: {name}, cleaned_name: {cleaned_name}")
#
#     # Search for mentions of the company name
#     mentions = []
#
#     # 1. Direct match of the cleaned name or original name
#     if cleaned_name in article.lower() or name.lower() in article.lower():
#         mentions.append((cleaned_name, name))
#
#     # 2. Match for words of the name in any order
#     name_words = cleaned_name.split()
#
#     # Create a regex pattern to find the name words in any order but still consecutively
#     phrase_pattern = r"\b(?:\w+\s+){0," + str(len(name_words) - 1) + r"}" + r"\s*".join(
#         re.escape(word) for word in name_words) + r"\b"
#
#     # Search for the pattern in the article
#     matches = re.finditer(phrase_pattern, article.lower())
#     for match in matches:
#         mentions.append((match.group(), name))
#
#     # If no mentions are found, return empty
#     if not mentions:
#         return None
#
#     # 3. Extract the context around each mention
#     context_snippets = []
#     for mention, original_name in mentions:
#         # Find position of the mention
#         mention_start = article.lower().find(mention)
#
#         # Calculate the context boundaries (before and after the mention)
#         start_pos = max(mention_start - window_size, 0)
#         end_pos = min(mention_start + len(mention) + window_size, len(article))
#
#         # Extract the snippet of context
#         context = article[start_pos:end_pos]
#         context_snippets.append(context)
#
#     return context_snippets


# def extract_context_around_mentions(article, name, window_words=25):
#     cleaned_name = remove_company_suffix(name.lower())
#     logger.info(f"name: {name}, cleaned_name: {cleaned_name}")
#
#     # Tokenize article into words while preserving original indices
#     article = preprocess_article(article)
#     words = word_tokenize(article)
#     word_indices = {i: words[i] for i in range(len(words))}
#
#     # Create regex pattern to find words in any order but still consecutively
#     name_words = cleaned_name.split()
#     phrase_pattern = r"\b(?:\w+\s+){0," + str(len(name_words) - 1) + r"}" + r"\s*".join(
#         re.escape(word) for word in name_words) + r"\b"
#
#     # Find all match positions (word-based index)
#     matches = [match.start() for match in re.finditer(phrase_pattern, article.lower())]
#
#     if not matches:
#         return None
#
#     # Convert character-based indices to word-based indices
#     word_match_indices = []
#     for match_pos in matches:
#         char_count = 0
#         for word_idx, word in word_indices.items():
#             char_count += len(word) + 1  # +1 for space
#             if char_count >= match_pos:
#                 word_match_indices.append(word_idx)
#                 break
#
#     # Extract context and check for overlap
#     context_snippets = []
#     last_start, last_end = -1, -1  # Track last extracted range
#
#     for match_word_idx in word_match_indices:
#         start_idx = max(match_word_idx - window_words, 0)
#         end_idx = min(match_word_idx + window_words, len(words))
#
#         # If no overlap, add a new snippet
#         if start_idx > last_end:
#             context_snippets.append(" ".join(words[start_idx:end_idx]))
#             last_start, last_end = start_idx, end_idx
#         else:
#             # Extend the last snippet if overlapping
#             last_snippet = context_snippets.pop()
#             new_start = min(last_start, start_idx)
#             new_end = max(last_end, end_idx)
#             context_snippets.append(" ".join(words[new_start:new_end]))
#             last_start, last_end = new_start, new_end
#
#     return context_snippets


import re
from itertools import permutations
from Levenshtein import distance
from unidecode import unidecode

import re

def remove_company_suffix(text):
    company_suffixes = [
        "ltd", "limited", "llc", "llp", "inc", "incorporated", "corp", "corporation",
        "gmbh", "ag", "ug", "ek", "sa", "spa", "sarl", "sas", "ptyltd",
        "bv", "nv", "kk", "as", "oy", "ab", "aps", "plc", "co",
        "sc", "cv", "spzoo", "zrt", "rt", "kft", "spolkaakcyjna",
        "ou", "ehf", "sadecv", "lc", "lllp", "lp", "pllc", "gp",
        "pc", "pty", "lda", "sdnbhd", "bhd", "sac", "eurl", "eirl", "jsc",
        "pjsc", "ojsc", "tov", "kf", "vof", "snc", "scs", "ee", "is","company"
    ]

    company_prefixes = [
        "branch of", "division of", "unit of", "subsidiary of",
        "department of", "office of", "affiliate of", "group of"
    ]

    # Remove prefix
    for prefix in company_prefixes:
        pattern_prefix = r"^\b" + re.escape(prefix) + r"\b[\s,:-]*"
        text = re.sub(pattern_prefix, "", text, flags=re.IGNORECASE)

    # Remove suffix
    pattern_suffix = r"([\s.,]+(" + "|".join(company_suffixes) + r"))+\b[\s.,]*$"
    text = re.sub(pattern_suffix, "", text, flags=re.IGNORECASE)

    return text.strip()

def clean_phrase(text):
    text = unidecode(text)
    name= re.sub(r"[^a-zA-Z0-9\s]", "", text)
    company_suffixes = [
        "ltd", "limited", "llc", "llp", "inc", "incorporated", "corp", "corporation",
        "gmbh", "ag", "ug", "ek", "sa", "spa", "sarl", "sas", "ptyltd",
        "bv", "nv", "kk", "as", "oy", "ab", "aps", "plc", "co",
        "sc", "cv", "spzoo", "zrt", "rt", "kft", "spolkaakcyjna",
        "ou", "ehf", "sadecv", "lc", "lllp", "lp", "pllc", "gp",
        "pc", "pty", "lda", "sdnbhd", "bhd", "sac", "eurl", "eirl", "jsc",
        "pjsc", "ojsc", "tov", "kf", "vof", "snc", "scs", "ee", "is",
        "company", "international", "branch", "division", "unit", "subsidiary",
        "department", "office", "affiliate", "group", "incorporation", "corporated",
        "services", "service", "trust", "firm", "organization", "org", "global",
        "co.", "corp.", "inc.", "ltd.", "affiliates", "subsidiaries", "branches",
        "entities", "entity", "offices", "departments", "national", "united", "spc",
    ]
    multi_word_stop_words=["single person company", "one person company"]
    stop_words = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 'nor',
                  'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet', 'with', 'of', 'was', 'are'}
    for words in multi_word_stop_words:
        if words in name:
            name = re.sub(r'\b' + re.escape(words) + r'\b', '', name, flags=re.IGNORECASE)

    words = [word for word in name.split() if (word.lower() not in stop_words and word.lower() not in company_suffixes) and len(word) > 2]
    return " ".join(word for word in words)
def clean_text(text):
    # remove latin letters and non alphaneumeric
    text = unidecode(text)
    # return text
    return re.sub(r"[^a-zA-Z0-9\s\.\,\!\?]", " ", text)



def generate_abbreviation(name):
    # abbreviation generated
    stop_words = {'a', 'an', 'and', 'as', 'at', 'but', 'by', 'for', 'if', 'in', 'nor',
                  'of', 'on', 'or', 'so', 'the', 'to', 'up', 'yet', 'with', 'co', 'inc',
                  'ltd', 'corp', 'company'}
    words = [word for word in name.split() if word.lower() not in stop_words and len(word) > 2]
    return "".join(word[0].upper() for word in words)

def get_name_variations(name):
    # variation of names generated
    variations = set()
    # variations = set([" ".join(perm) for perm in permutations(words, len(words))])  # Different word orders
    variations.add(name)  # Add the original name
    abb= generate_abbreviation(name)
    if len(abb)>1:
        variations.add(abb)  # Add abbreviation
    return variations

def levenshtein_similarity(s1, s2):
    # calculate LD
    max_len = max(len(s1), len(s2))
    if max_len == 0:
        return 100
    return (1 - (distance(s1, s2) / max_len)) * 100

# def extract_context_around_mentions(article, name, window_words=25, similarity_threshold=80):
#
#     # Clean name and article
#     cleaned_name = clean_text(name.lower())
#     cleaned_name = remove_company_suffix(cleaned_name)
#     cleaned_article = clean_text(article.lower())
#
#     # Generate name variations
#     name_variations = get_name_variations(cleaned_name)
#     print(f"Name variations: ------> {name_variations}")
#
#     # split the cleaned article
#     words = cleaned_article.split()
#
#     # Find fuzzy matches in the article
#     context_snippets = []
#     for i in range(len(words)):
#         for variation in name_variations:
#             var_length = len(variation.split())
#             if i + var_length <= len(words):  # to check the bounds
#                 sub_text = " ".join(words[i:i + var_length]) # Circular : the Boston Consulting group
#                 similarity = levenshtein_similarity(sub_text, variation)
#
#                 if similarity >= similarity_threshold:
#                     print("similarity------>", similarity)
#
#                     start_idx = max(i - window_words, 0) # Ensure it's positive integer
#                     end_idx = min(i + window_words, len(words)) #Ensure it's not beyond the length
#                     snippet = " ".join(words[start_idx:end_idx])
#
#                     if len(context_snippets) >=5:
#                         break
#                     if snippet not in context_snippets:
#                         context_snippets.append(snippet)
#
#     return context_snippets

def extract_context_around_mentions(article, name, window_words=25, similarity_threshold=65):

    # Clean name and article
    cleaned_name = clean_phrase(name.lower())
    # cleaned_name = remove_company_suffix(cleaned_name)

    cleaned_article = clean_text(article.lower())

    # Generate name variations
    name_variations = get_name_variations(cleaned_name)
    logger.info(name_variations)
    # print(f"Name variations: ------> {name_variations}")

    # split the cleaned article
    # words = cleaned_article.split()
    sentences = re.split(r'(?<=[.!?])\s+', cleaned_article)

    # Find fuzzy matches in the article
    context_snippets = []
    for sentence in sentences:
        sentence = clean_phrase(sentence)
        words = sentence.split()
        for i in range(len(words)):
            for variation in name_variations:
                found_match = False
                var_length = len(variation.split())
                if i + var_length <= len(words):  # to check the bounds
                    sub_text = " ".join(words[i:i + var_length]) # Circular : the Boston Consulting group
                    similarity = levenshtein_similarity(sub_text, variation)

                    if similarity >= similarity_threshold:
                        # print("similarity------>", similarity)
                        snippet = sentence

                        if len(context_snippets) >=5:
                            break
                        if snippet not in context_snippets:
                            context_snippets.append(snippet)
                        found_match = True
                        break
            if found_match==True:
                break

    return context_snippets

