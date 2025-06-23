import json
import time
from urllib.parse import quote, urlparse
import requests
from selectolax.parser import HTMLParser
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Suppress only the InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import concurrent.futures

session = requests.Session()
session.verify = False  # Not recommended for production



def get_base64_str(source_url):
    """
    Extracts the base64 string from a Google News URL.

    Parameters:
        source_url (str): The Google News article URL.

    Returns:
        dict: A dictionary containing 'status' and 'base64_str' if successful,
              otherwise 'status' and 'message'.
    """
    try:
        url = urlparse(source_url)
        path = url.path.split("/")
        if (
            url.hostname == "news.google.com"
            and len(path) > 1
            and path[-2] in ["articles", "read"]
        ):
            return {"status": True, "base64_str": path[-1]}
        return {"status": False, "message": "Invalid Google News URL format."}
    except Exception as e:
        return {"status": False, "message": f"Error in get_base64_str: {str(e)}"}


def get_decoding_params(base64_str):
    """
    Fetches signature and timestamp required for decoding from Google News.
    It first tries to use the URL format https://news.google.com/articles/{base64_str},
    and falls back to https://news.google.com/rss/articles/{base64_str} if any error occurs.

    Parameters:
        base64_str (str): The base64 string extracted from the Google News URL.

    Returns:
        dict: A dictionary containing 'status', 'signature', 'timestamp', and 'base64_str' if successful,
              otherwise 'status' and 'message'.
    """
    # Try the first URL format.
    try:
        url = f"https://news.google.com/articles/{base64_str}"
        response = requests.get(url, verify=False)
        response.raise_for_status()

        parser = HTMLParser(response.text)
        data_element = parser.css_first("c-wiz > div[jscontroller]")
        if data_element is None:
            return {
                "status": False,
                "message": "Failed to fetch data attributes from Google News with the articles URL.",
            }

        return {
            "status": True,
            "signature": data_element.attributes.get("data-n-a-sg"),
            "timestamp": data_element.attributes.get("data-n-a-ts"),
            "base64_str": base64_str,
        }

    except requests.exceptions.RequestException as req_err:
        # If an error occurs, try the fallback URL format.
        try:
            url = f"https://news.google.com/rss/articles/{base64_str}"
            response = requests.get(url, verify=False)
            response.raise_for_status()

            parser = HTMLParser(response.text)
            data_element = parser.css_first("c-wiz > div[jscontroller]")
            if data_element is None:
                return {
                    "status": False,
                    "message": "Failed to fetch data attributes from Google News with the RSS URL.",
                }

            return {
                "status": True,
                "signature": data_element.attributes.get("data-n-a-sg"),
                "timestamp": data_element.attributes.get("data-n-a-ts"),
                "base64_str": base64_str,
            }

        except requests.exceptions.RequestException as rss_req_err:
            return {
                "status": False,
                "message": f"Request error in get_decoding_params with RSS URL: {str(rss_req_err)}",
            }
    except Exception as e:
        return {
            "status": False,
            "message": f"Unexpected error in get_decoding_params: {str(e)}",
        }


def decode_url(signature, timestamp, base64_str):
    """
    Decodes the Google News URL using the signature and timestamp.

    Parameters:
        signature (str): The signature required for decoding.
        timestamp (str): The timestamp required for decoding.
        base64_str (str): The base64 string from the Google News URL.

    Returns:
        dict: A dictionary containing 'status' and 'decoded_url' if successful,
              otherwise 'status' and 'message'.
    """
    try:
        url = "https://news.google.com/_/DotsSplashUi/data/batchexecute"
        payload = [
            "Fbv4je",
            f'["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,null,0],"{base64_str}",{timestamp},"{signature}"]',
        ]
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
        }

        response = requests.post(
            url, headers=headers, data=f"f.req={quote(json.dumps([[payload]]))}", verify=False
        )
        response.raise_for_status()

        parsed_data = json.loads(response.text.split("\n\n")[1])[:-2]
        decoded_url = json.loads(parsed_data[0][2])[1]

        return {"status": True, "decoded_url": decoded_url}
    except requests.exceptions.RequestException as req_err:
        return {
            "status": False,
            "message": f"Request error in decode_url: {str(req_err)}",
        }
    except (json.JSONDecodeError, IndexError, TypeError) as parse_err:
        return {
            "status": False,
            "message": f"Parsing error in decode_url: {str(parse_err)}",
        }
    except Exception as e:
        return {"status": False, "message": f"Error in decode_url: {str(e)}"}


def decode_google_news_url(source_query, interval=None):
    """
    Decodes a Google News article URL into its original source URL.

    Parameters:
        source_url (str): The Google News article URL.
        interval (int, optional): Delay time in seconds before decoding to avoid rate limits.

    Returns:
        dict: A dictionary containing 'status' and 'decoded_url' if successful,
              otherwise 'status' and 'message'.
    """
    # interval_time = 5

    source_url = source_query.get("link")
    try:
        base64_response = get_base64_str(source_url)
        if not base64_response["status"]:
            return base64_response

        decoding_params_response = get_decoding_params(base64_response["base64_str"])
        if not decoding_params_response["status"]:
            return decoding_params_response

        decoded_url_response = decode_url(
            decoding_params_response["signature"],
            decoding_params_response["timestamp"],
            decoding_params_response["base64_str"],
        )
        if interval:
            time.sleep(interval)

        compiled_response = source_query
        compiled_response['decoding'] = decoded_url_response

        return compiled_response
    except Exception as e:
        return {
            "status": False,
            "message": f"Error in decode_google_news_url: {str(e)}",
        }


def execute_decoding_concurrently(source_queries):
    """
    Concurrently performs the url decoding on the provided google news links
    :param source_queries: news list of dicts
    :return: news list of dicts with decoded url to the base news website
    """

    # Using ThreadPoolExecutor for I/O-bound tasks
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(decode_google_news_url, arg): arg for arg in source_queries}

        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)

    return results
