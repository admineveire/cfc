from datetime import datetime, timezone

from app.database import get_database

SETTINGS_COLLECTION = "app_settings"
LOAN_CALCULATOR_SETTINGS_ID = "loan_calculator"
DEFAULT_ANNUAL_INTEREST_RATE = 6.5


async def get_annual_interest_rate() -> float:
    collection = get_database()[SETTINGS_COLLECTION]
    doc = await collection.find_one({"_id": LOAN_CALCULATOR_SETTINGS_ID})
    if not doc:
        return DEFAULT_ANNUAL_INTEREST_RATE

    value = doc.get("annual_interest_rate", DEFAULT_ANNUAL_INTEREST_RATE)
    try:
        rate = float(value)
    except (TypeError, ValueError):
        return DEFAULT_ANNUAL_INTEREST_RATE

    if rate < 0:
        return DEFAULT_ANNUAL_INTEREST_RATE

    return rate


async def set_annual_interest_rate(rate: float) -> None:
    collection = get_database()[SETTINGS_COLLECTION]
    await collection.update_one(
        {"_id": LOAN_CALCULATOR_SETTINGS_ID},
        {
            "$set": {
                "annual_interest_rate": float(rate),
                "updated_at": datetime.now(timezone.utc),
            }
        },
        upsert=True,
    )
