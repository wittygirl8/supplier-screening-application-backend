import enum
import uuid
from datetime import datetime
from sqlalchemy.sql import expression
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import ARRAY, BigInteger, Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, Uuid, func, Enum as SQLAlchemyEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class ValidationStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    PENDING = "PENDING"

class FinalStatus(enum.Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"

class FinalValidatedStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING  ="PENDING"
    FAILED = "FAILED"
    AUTO_REJECT = "AUTO_REJECT"
    AUTO_ACCEPT = "AUTO_ACCEPT"
    REVIEW = "REVIEW"

class OribisMatchStatus(enum.Enum):
    MATCH = "MATCH"
    NO_MATCH = "NO_MATCH"
    PENDING = "PENDING"
    
class TruesightStatus(enum.Enum):
    VALIDATED = "VALIDATED"
    NOT_VALIDATED = "NOT_VALIDATED"
    NOT_REQUIRED = "NOT_REQUIRED"
    PENDING = "PENDING"
    NO_MATCH = "NO_MATCH"


class STATUS(str, enum.Enum):  
    QUEUED = "QUEUED" # Added new 
    NOT_STARTED = "NOT_STARTED"
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS" 
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"

class DUPINSESSION(str, enum.Enum):
    RETAIN = "RETAIN"
    REMOVE = "REMOVE"
    UNIQUE = "UNIQUE"

class Base(DeclarativeBase):
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class User(Base):
    __tablename__ = "user_account"

    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    email: Mapped[str] = mapped_column(
        String(256), nullable=False, unique=True, index=True
    )
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")

class Base(DeclarativeBase):
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    update_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class UserAccount(Base):
    __tablename__ = "user_account"

    user_id: Mapped[str] = mapped_column(
        Uuid(as_uuid=False), primary_key=True, default=lambda _: str(uuid.uuid4())
    )
    email: Mapped[str] = mapped_column(
        String(256), nullable=False, unique=True, index=True
    )
    hashed_password: Mapped[str] = mapped_column(String(128), nullable=False)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")


class User(Base):
    __tablename__ = "users_table"

    id = Column(Integer, autoincrement=True, index=True)
    user_id= Column( String, primary_key=True, unique = True)
    email = Column(String, unique=True, nullable=False, index=True)
    username = Column(String, unique=True, nullable=False, index=True)
    password = Column(String, nullable=False)
    verified = Column(Boolean, default=False, nullable=False)
    otp = Column(String, nullable=True)
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(back_populates="user")
    user_group = Column(String, nullable=False)
    api_key = Column(String, unique=True, nullable=False)
    key_expires_at = Column(DateTime, nullable=True)
    def __repr__(self):
        return f"<User(id={self.id}, user_id='{self.user_id}', email='{self.email}', username='{self.username}')>"

class RefreshToken(Base):
    __tablename__ = "refresh_token"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    refresh_token: Mapped[str] = mapped_column(
        String(512), nullable=False, unique=True, index=True
    )
    used: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    exp: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id = Column(String, ForeignKey("users_table.user_id", ondelete="CASCADE"))  # Change to correct PK
    user: Mapped["User"] = relationship(back_populates="refresh_tokens")
    user_group: Mapped[str] = Column(String, nullable=True) 

class UploadSupplierMasterData(Base):
    __tablename__ = "upload_supplier_master_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    unmodified_name = Column(String, nullable=True)
    unmodified_name_international = Column(String, nullable=True)
    unmodified_address = Column(Text, nullable=True)
    unmodified_postcode = Column(String, nullable=True)
    unmodified_city = Column(String, nullable=True)
    unmodified_country = Column(String, nullable=True)
    unmodified_phone_or_fax = Column(String, nullable=True)
    unmodified_email_or_website = Column(String, nullable=True)
    unmodified_national_id = Column(String, nullable=True)
    unmodified_state = Column(String, nullable=True)
    unmodified_address_type = Column(String, nullable=True)
    uploaded_name = Column(String, nullable=True)
    uploaded_name_international = Column(String, nullable=True)
    uploaded_address = Column(Text, nullable=True)
    uploaded_postcode = Column(String, nullable=True)
    uploaded_city = Column(String, nullable=True)
    uploaded_country = Column(String, nullable=True)
    uploaded_phone_or_fax = Column(String, nullable=True)
    uploaded_email_or_website = Column(String, nullable=True)
    uploaded_national_id = Column(String, nullable=True)
    uploaded_state = Column(String, nullable=True)
    uploaded_address_type = Column(String, nullable=True)
    name = Column(String, nullable=True)
    name_international = Column(String, nullable=True)
    address = Column(Text, nullable=True)
    postcode = Column(String, nullable=True)
    city = Column(String, nullable=True)
    country = Column(String, nullable=True)
    phone_or_fax = Column(String, nullable=True)
    email_or_website = Column(String, nullable=True)
    national_id = Column(String, nullable=True)
    state = Column(String, nullable=True)
    address_type = Column(String, nullable=True)
    suggested_name = Column(String, nullable=True)
    suggested_name_international = Column(String, nullable=True)
    suggested_address = Column(Text, nullable=True)
    suggested_postcode = Column(String, nullable=True)
    suggested_city = Column(String, nullable=True)
    suggested_country = Column(String, nullable=True)
    suggested_phone_or_fax = Column(String, nullable=True)
    suggested_email_or_website = Column(String, nullable=True)
    suggested_national_id = Column(String, nullable=True)
    suggested_state = Column(String, nullable=True)
    suggested_address_type = Column(String, nullable=True)
    ens_id = Column(String, primary_key=True,nullable=False)
    session_id = Column(String, nullable=False)
    bvd_id = Column(String, nullable=True)
    validation_status = Column(SQLAlchemyEnum(ValidationStatus), nullable=False,server_default=expression.literal(ValidationStatus.PENDING.value))
    final_status = Column(SQLAlchemyEnum(FinalStatus), nullable=False,server_default=expression.literal(FinalStatus.PENDING.value))
    #new
    final_validation_status = Column(SQLAlchemyEnum(FinalValidatedStatus), nullable=False,server_default=expression.literal(FinalValidatedStatus.PENDING.value))
    orbis_matched_status = Column(SQLAlchemyEnum(OribisMatchStatus), nullable=False,server_default=expression.literal(OribisMatchStatus.PENDING.value))

    truesight_status = Column(SQLAlchemyEnum(TruesightStatus), nullable=False,  server_default=expression.literal(TruesightStatus.PENDING.value))
    matched_percentage = Column(Integer, nullable=False, default=0)
    truesight_percentage = Column(Integer, nullable=False, default=0)
    suggested_bvd_id = Column(String, nullable=True)
    pre_existing_bvdid = Column(Boolean, default=False, nullable=False)
    process_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.PENDING.value))
    user_id = Column(String, ForeignKey("users_table.user_id", ondelete="CASCADE"))  # Change to correct PK
    uploaded_external_vendor_id = Column(String, nullable=True)
    duplicate_in_session = Column(SQLAlchemyEnum(DUPINSESSION), nullable=False, server_default=expression.literal(DUPINSESSION.UNIQUE.value))


class SupplierMasterData(Base):
    __tablename__ = "supplier_master_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=True)
    uploaded_name = Column(String(255), nullable=True)
    external_vendor_id = Column(String, nullable=True)
    name_international = Column(String(255), nullable=True)
    address = Column(Text, nullable=True)
    postcode = Column(String(20), nullable=True)
    city = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    phone_or_fax = Column(String(50), nullable=True)
    email_or_website = Column(String(100), nullable=True)
    national_id = Column(String(50), nullable=True)
    state = Column(String(100), nullable=True)
    ens_id = Column(String(50), nullable=True)
    session_id = Column(String(50), nullable=False)
    bvd_id = Column(String(50), nullable=False)
    validation_status = Column(SQLAlchemyEnum(ValidationStatus), nullable=False,server_default=expression.literal(ValidationStatus.PENDING.value))
    report_generation_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    final_status = Column(SQLAlchemyEnum(FinalStatus), nullable=False,server_default=expression.literal(FinalStatus.PENDING.value))
    # Ensure unique constraint for (ens_id, session_id)
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', name='supplier_master_ensid_session'),
    )

class ExternalSupplierData(Base):
    __tablename__ = "external_supplier_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    roe_using_net_income = Column(JSONB, nullable=True)
    event_adverse_media_other_crimes = Column(JSONB, nullable=True)
    event_adverse_media_reputational_risk = Column(JSONB, nullable=True)
    legal = Column(JSONB, nullable=True)
    solvency_ratio = Column(JSONB, nullable=True)
    roce_before_tax = Column(JSONB, nullable=True)
    roe_before_tax = Column(JSONB, nullable=True)
    profit_margin = Column(JSONB, nullable=True)
    shareholders_fund = Column(JSONB, nullable=True)
    total_assets = Column(JSONB, nullable=True)
    cash_flow = Column(JSONB, nullable=True)
    pl_before_tax = Column(JSONB, nullable=True)
    beneficial_owners_intermediatory = Column(JSONB, nullable=True)
    controlling_shareholders = Column(JSONB, nullable=True)
    grid_event_sanctions = Column(JSONB, nullable=True)
    grid_event_regulatory = Column(JSONB, nullable=True)
    grid_event_bribery_fraud_corruption = Column(JSONB, nullable=True)
    grid_event_pep = Column(JSONB, nullable=True)
    grid_event_adverse_media_other_crimes = Column(JSONB, nullable=True)
    grid_event_adverse_media_reputational_risk = Column(JSONB, nullable=True)
    grid_legal = Column(JSONB, nullable=True)
    management = Column(JSONB, nullable=True)
    no_of_employee = Column(Integer, nullable=True)
    national_identifier_type = Column(JSONB, nullable=True)
    national_identifier = Column(JSONB, nullable=True)
    alias = Column(JSONB, nullable=True)
    incorporation_date = Column(Date, nullable=True)
    num_subsidiaries = Column(Integer, nullable=True)
    num_companies_in_corp_grp = Column(Integer, nullable=True)
    num_direct_shareholders = Column(Integer, nullable=True)
    operating_revenue = Column(JSONB, nullable=True)
    profit_loss_after_tax = Column(JSONB, nullable=True)
    ebitda = Column(JSONB, nullable=True)
    current_ratio = Column(JSONB, nullable=True)
    pr_qualitative_score_date = Column(Date, nullable=True)
    pr_more_risk_score_date = Column(Date, nullable=True)
    pr_reactive_more_risk_score_date = Column(Date, nullable=True)
    payment_risk_score = Column(Numeric, nullable=True)
    esg_overall_rating = Column(Integer, nullable=True)
    esg_environmental_rating = Column(Integer, nullable=True)
    esg_social_rating = Column(Integer, nullable=True)
    esg_governance_rating = Column(Integer, nullable=True)
    esg_date = Column(Date, nullable=True)
    cyber_risk_score = Column(Integer, nullable=True)
    cyber_date = Column(Date, nullable=True)
    implied_cyber_risk_score_date = Column(Date, nullable=True)
    beneficial_owners = Column(JSONB, nullable=True)
    global_ultimate_owner = Column(JSONB, nullable=True)
    shareholders = Column(JSONB, nullable=True)
    ultimately_owned_subsidiaries = Column(JSONB, nullable=True)
    other_ultimate_beneficiary = Column(JSONB, nullable=True)
    event_sanctions = Column(JSONB, nullable=True)
    event_regulatory = Column(JSONB, nullable=True)
    event_bribery_fraud_corruption = Column(JSONB, nullable=True)
    event_pep = Column(JSONB, nullable=True)
    name = Column(Text, nullable=True)
    country = Column(Text, nullable=True) #Text
    location = Column(Text, nullable=True) #Text
    address = Column(Text, nullable=True)
    is_active = Column(String, nullable=True)
    operation_type = Column(String, nullable=True)
    legal_form = Column(String, nullable=True)
    bvd_id = Column(String, nullable=True)
    pr_reactive_more_risk_score = Column(String, nullable=True)
    cyber_botnet_infection = Column(String, nullable=True)
    cyber_malware_servers = Column(String, nullable=True)
    cyber_ssl_certificate = Column(String, nullable=True)
    cyber_webpage_headers = Column(String, nullable=True)
    website = Column(Text, nullable=True)
    implied_cyber_risk_score = Column(Text, nullable=True)
    ens_id = Column(String, nullable=True)
    controlling_shareholders_type = Column(JSONB, nullable=True)
    session_id = Column(String, nullable=True)
    cyber_bonet_infection = Column(String, nullable=True)
    global_ultimate_owner_type = Column(JSONB, nullable=True)
    pr_qualitative_score = Column(String, nullable=True)
    pr_more_risk_score = Column(String, nullable=True)
    pr_more_risk_score_ratio = Column(JSONB, nullable=True)
    pr_reactive_more_risk_score_ratio = Column(JSONB, nullable=True)
    long_and_short_term_debt = Column(Numeric, nullable=True)
    long_term_debt = Column(Numeric, nullable=True)
    total_shareholders_equity = Column(Numeric, nullable=True)
    default_events = Column(JSONB, nullable=True)
    orbis_news = Column(JSONB, nullable=True)
    operating_revenue_usd = Column(JSONB, nullable=True)
    event = Column(JSONB, nullable=True)
    # Add the UniqueConstraint on the combination of ens_id and session_id
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', name='unique_ens_session'),
    )

class KPISchemas(Base):    
    __abstract__ = True
    id = Column(Integer, primary_key=True, autoincrement=True) # Unique identifier for each record
    kpi_area = Column(String, nullable=False)                  # Area of KPI (e.g., ESG)
    kpi_code = Column(String, nullable=False)                  # Unique code for the KPI
    kpi_flag = Column(Boolean, nullable=False, default=False)  # Boolean flag for the KPI
    kpi_value = Column(String, nullable=True)                   # Numeric value associated with the KPI
    kpi_details = Column(String, nullable=True)                # Additional details for the KPI
    ens_id = Column(String, nullable=False)                    # Ensures related entity ID
    session_id = Column(String, nullable=False)                # Session identifier
    kpi_rating = Column(String, nullable=True) 
    kpi_definition = Column(String, nullable=True) 
    


class KpiCyes(KPISchemas):
    __tablename__ = "cyes"
    # Ensure unique constraint for (ens_id, session_id, kpi_code)
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpicyes'),
    )
    

class KpiFstb(KPISchemas):
    __tablename__ = "fstb"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpifstb'),
    )
    

class KpiLgrk(KPISchemas):
    __tablename__ = "lgrk"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpilgrk'),
    )
    

class KpiNews(KPISchemas):
    __tablename__ = "news"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpinews'),
    )


class KpiOval(KPISchemas):
    __tablename__ = "oval"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpioval'),
    )

class KpiOvar(KPISchemas):
    __tablename__ = "ovar"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpiovar'),
    )

class KpiRfct(KPISchemas):
    __tablename__ = "rfct"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpirfct'),
    )
class KpiSape(KPISchemas):
    __tablename__ = "sape"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpisape'),
    )
    
class KpiSown(KPISchemas):
    __tablename__ = "sown"
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', 'kpi_code', name='unique_ensid_session_kpisown'),
    )
class EnsidScreeningStatus(Base):
    __tablename__ = "ensid_screening_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    overall_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    orbis_retrieval_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    screening_modules_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    report_generation_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    # Ensure unique constraint for (ens_id, session_id)
    __table_args__ = (
        UniqueConstraint('ens_id', 'session_id', name='unique_ensid_session_ensid_screening_status'),
    )
class SessionScreeningStatus(Base):
    __tablename__ = "session_screening_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    overall_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    list_upload_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    supplier_name_validation_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    screening_analysis_status = Column(SQLAlchemyEnum(STATUS), nullable=False, server_default=expression.literal(STATUS.NOT_STARTED.value))
    # Ensure unique constraint for (ens_id, session_id)
    __table_args__ = (
        UniqueConstraint('session_id', name='unique_sessionid_session'),
    )


class GridManagement(Base):
    __tablename__ = "grid_management"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    contact_id = Column(String, nullable=True)
    grid_adverse_media_other_crimes = Column(JSONB, nullable=True)
    grid_adverse_media_reputational_risk = Column(JSONB, nullable=True)
    grid_sanctions = Column(JSONB, nullable=True)
    grid_regulatory = Column(JSONB, nullable=True)
    grid_bribery_fraud_corruption = Column(JSONB, nullable=True)
    grid_pep = Column(JSONB, nullable=True)
    grid_legal = Column(JSONB, nullable=True)
class CompanyProfile(Base):
    __tablename__ = "company_profile"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Primary key added
    name = Column(String, nullable=True) 
    uploaded_name = Column(String(255), nullable=True)
    external_vendor_id = Column(String, nullable=True) 
    location = Column(String, nullable=True)
    address = Column(String, nullable=True)    
    website = Column(String, nullable=True)    
    active_status = Column(String, nullable=True)    
    operation_type = Column(String, nullable=True)    
    legal_status = Column(String, nullable=True)    
    national_identifier = Column(String, nullable=True)    
    alias = Column(Text, nullable=True)    
    incorporation_date = Column(String, nullable=True)    
    shareholders = Column(Text, nullable=True)    
    revenue = Column(String, nullable=True)    
    subsidiaries = Column(String, nullable=True)    
    corporate_group = Column(String, nullable=True)    
    key_executives = Column(Text, nullable=True)    
    employee = Column(String, nullable=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=False)

    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", name="unique_ensid_session"),
    )


class SentimentPlot(Base):
    __tablename__ = "report_plot"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    sentiment_aggregation = Column(JSONB)
class NewsMaster(Base):
    __tablename__ = "news_master"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(Text)
    name = Column(String, nullable=False)
    title = Column(Text)
    category = Column(Text)
    summary = Column(Text)
    news_date = Column(Date)
    sentiment = Column(String)
    content_filtered = Column(Boolean)
    country = Column(Text)
    start_date = Column (Date)
    end_date = Column(Date)
    error = Column(Text, nullable=True)
    __table_args__ = (
        UniqueConstraint("name", "link", "news_date", name="unique_name_link_date"),
    )

class Summary(Base):
    __tablename__ = "summary"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(50), nullable=False)
    ens_id = Column(String(50), nullable=True)
    area = Column(String(50), nullable=False) 
    summary = Column(Text)

    __table_args__ = (
        UniqueConstraint("ens_id", "session_id", "area", name="unique_ens_session_summary"),
    )


class ClientConfiguration(Base):
    __tablename__ = "client_configuration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), nullable=False)
    client_name = Column(String(250), nullable=True)
    kpi_theme = Column(String(50), nullable=True)
    report_section = Column(String(50), nullable=True)
    kpi_area = Column(String, nullable=False)                  
    module_enabled_status =  mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("client_id", "client_name", "kpi_theme", "report_section", "kpi_area", name="unique_client_configuration"),
    )


class SessionConfiguration(Base):
    __tablename__ = "session_configuration"

    id = Column(Integer, primary_key=True, autoincrement=True)
    client_id = Column(String(50), nullable=False)
    session_id = Column(String(50), nullable=False)
    module = Column(String(50), nullable=True)
    module_active_status = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("session_id", "module", name="unique_session_configuration"),
    )

class TokenMonitor(Base):
    __tablename__ = "token_monitor"

    id = Column(Integer, primary_key=True, autoincrement=True)
    usage_time = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    token_used = Column(Integer, nullable=False)
    payload = Column(JSONB, nullable=True)
    openai_model = Column(Text, nullable=True)

class EntityUniverse(Base):
    __tablename__ = "entity_universe"

    id = Column(Integer, autoincrement=True)
    ens_id = Column(String(50), primary_key=True)
    bvd_id = Column(String(50), nullable=True)
    name = Column(String(255), nullable=True)
    name_international = Column(String(255), nullable=True)
    address = Column(Text, nullable=True)
    postcode = Column(String(20), nullable=True)
    city = Column(String(100), nullable=True)
    country = Column(String(100), nullable=True)
    phone_or_fax = Column(String(50), nullable=True)
    email_or_website = Column(String(100), nullable=True)
    national_id = Column(String(50), nullable=True)
    state = Column(String(100), nullable=True)
    address_type = Column(String(50), nullable=True)
    last_session_id = Column(String(50), nullable=True)
    overall_supplier_rating = Column(String(50), nullable=True)
    thematic_rating = Column(JSONB, nullable=True)

class WebhookResponse(Base):
    __tablename__ = "webhook_response"

    id = Column(Integer,  primary_key=True, autoincrement=True)
    response = Column(String(50))
    response_time = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

class GridPMTracking(Base):
    __tablename__ = "grid_PM_tracking"

    id = Column(Integer, autoincrement=True)
    grid_tracking_id = Column(String, primary_key=True)
    category = Column(String)
    grid_enquiry_id = Column(String)
    primary_id = Column(String)

class ManagementAssociation(Base):
    __tablename__ = "management_association"

    id = Column(Integer, autoincrement=True)
    contact_id = Column(String, primary_key=True)
    name = Column(String)
    associated_ens_id = Column(ARRAY(String))

class EnsContinuousMonitoring(Base):
    __tablename__ = "ens_continuous_monitoring"

    id = Column(Integer, autoincrement=True)
    ens_id = Column(String(50), primary_key=True)
    group_id = Column(String(50))
    last_screened_time = Column(DateTime)
    name = Column(String(255))
    name_international = Column(String(255))
    address = Column(Text)
    postcode = Column(String(20))
    city = Column(String(100))
    country = Column(String(100))
    phone_or_fax = Column(String(50))
    email_or_website = Column(String(100))
    national_id = Column(String(50))
    state = Column(String(100))


class ExcludedEntities(Base):
    __tablename__ = "excluded_entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100))
    category = Column(Text)


class APIKey(Base):
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    api_key = Column(String, unique=True, nullable=False)
    user_id = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    expires_at = Column(DateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint('user_id', 'api_key', name='api_keys_unique'),
    )