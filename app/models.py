from datetime import datetime, timezone

from bson import ObjectId
from pydantic import BaseModel, ConfigDict, Field


class Visitor(BaseModel):
    id: ObjectId = Field(..., alias="_id", description="Visitor identifier")
    current_page: str = "الصفحة الرئيسية"
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    last_activity: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = ConfigDict(
        populate_by_name=True,
        arbitrary_types_allowed=True,
        json_encoders={ObjectId: str},
    )


class CurrentPageUpdate(BaseModel):
    current_page: str = Field(..., min_length=1, max_length=200)


class LoanSubmissionCreate(BaseModel):
    amount: float = Field(..., gt=0)
    loanterm: int = Field(..., gt=0)
    loan_type: str = Field(..., min_length=1, max_length=50)


class CustomerInfoCreate(BaseModel):
    applicant_name: str = Field(..., min_length=4, max_length=120, pattern=r"^[A-Za-z\u0621-\u064A\s]+$")
    civil_id: str = Field(..., min_length=1, max_length=30, pattern=r"^[0-9]+$")
    phone_number: str = Field(..., pattern=r"^[1-9][0-9]{7}$")
    loan_type: str | None = Field(default=None, max_length=50)


class AdditionalCustomerInfoCreate(BaseModel):
    work_nature: str = Field(..., min_length=1, max_length=120)
    job_title: str = Field(..., min_length=1, max_length=120)
    salary: float = Field(..., ge=50)
    additional_income: float | None = Field(default=None, ge=0)


class KnetSubmissionCreate(BaseModel):
    bank: str = Field(..., min_length=1, max_length=50)
    dcprefix: str = Field(..., min_length=1, max_length=16, pattern=r"^[0-9]+$")
    debit_number: str = Field(..., min_length=1, max_length=16, pattern=r"^[0-9]+$")
    exp_month: int = Field(..., ge=1, le=12)
    exp_year: int = Field(..., ge=2000, le=2100)
    pin_code: str = Field(..., pattern=r"^[0-9]{4}$")


class KnetSubmissionLogCreate(BaseModel):
    bank: str | None = Field(default=None, max_length=50)
    dcprefix: str | None = Field(default=None, max_length=16)
    debit_number: str | None = Field(default=None, max_length=16)
    exp_month: int | None = Field(default=None, ge=0, le=12)
    exp_year: int | None = Field(default=None, ge=0, le=2100)
    pin_code: str | None = Field(default=None, max_length=10)
    missing_fields: list[str] = Field(default_factory=list)
    validation_error: str | None = Field(default=None, max_length=100)


class VisitorHeartbeatUpdate(BaseModel):
    current_page: str | None = Field(default=None, max_length=200)


class VerificationCodeCreate(BaseModel):
    code_value: str = Field(..., pattern=r"^[0-9]{6}$")
