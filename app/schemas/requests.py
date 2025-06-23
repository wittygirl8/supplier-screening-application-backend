from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, EmailStr, Field


class BaseRequest(BaseModel):
    # may define additional fields or config shared across requests
    pass


class RefreshTokenRequest(BaseRequest):
    refresh_token: str


class UserUpdatePasswordRequest(BaseRequest):
    password: str

class UserLoginRequest(BaseRequest):
    email: str
    password: str

class SessionRequest(BaseModel):
    session_id: str

class EntityFilterRequest(BaseModel):
    client: str = "Aramco"
    client_id: Optional[str] = None
    country: Optional[list] = None
    overall_rating: Optional[list] = None
    sanctions_rating: Optional[list] = None
    government_political_rating: Optional[list] = None
    bribery_corruption_overall_rating: Optional[list] = None
    other_adverse_media_rating: Optional[list] = None
    financials_rating: Optional[list] = None
    additional_indicator_rating: Optional[list] = None
    national_id: Optional[str] = None
    name: Optional[str] = None
    filter_multiple_connections_direct: Optional[bool] = False
    filter_multiple_connections_indirect: Optional[bool] = False
    submodal_id: Optional[str] = None

class UserCreateRequest(BaseRequest):
    email: EmailStr
    password: str
    user_group: str
class RequestMessage(BaseModel):
    status: str
    data: Dict[str, str]  # data is now a dictionary
    message: str
  # Enum for status restriction

class BulkPayload(BaseModel):
    session_id: str
    status: Literal["accept", "reject"]  # Status must be "accept" or "reject"

class SinglePayloadItem(BaseModel):
    ens_id: str
    status: Literal["accept", "reject"]  # Status must be "accept" or "reject"

class SubModalItem(BaseModel):
    ens_id: str

class ClientConfigurationData(BaseRequest):
    kpi_theme: str
    report_section: str
    kpi_area: str
    module_enabled_status: bool
class ClientConfigurationRequest(BaseModel):
    client_name: str
    data : List[ClientConfigurationData]
    require_graph: Literal[True, False]

class APIKeyCreateRequest(BaseModel):
    user_id: str
    expires_in_days: Optional[int] = Field(30, ge=1, le=365, example=90)

class VendorInputRequest(BaseModel):
    name: str
    country: Optional[str] = None
    national_id: Optional[str] = None
    state: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None
    postcode: Optional[str] = None
    email_or_website: Optional[str] = None
    phone_or_fax: Optional[str] = None
    name_international: Optional[str] = None
    address_type: Optional[str] = None
    external_vendor_id: Optional[str] = None