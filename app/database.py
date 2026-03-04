import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import DESCENDING
from app.presence import start_presence, stop_presence
from app.realtime import start_realtime_bus, stop_realtime_bus

load_dotenv()


class MongoState:
    client: AsyncIOMotorClient | None = None


mongo_state = MongoState()


def get_database() -> AsyncIOMotorDatabase:
    if mongo_state.client is None:
        raise RuntimeError("MongoDB client is not initialized.")

    db_name = os.getenv("DB_NAME", "visitors")
    return mongo_state.client[db_name]


async def _deduplicate_customer_info_by_visitor(db: AsyncIOMotorDatabase) -> None:
    customer_info_collection = db["customer_info"]
    duplicate_cursor = customer_info_collection.aggregate(
        [
            {"$group": {"_id": "$visitor_id", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ]
    )

    async for duplicate_entry in duplicate_cursor:
        visitor_id = duplicate_entry.get("_id")
        if visitor_id is None:
            continue

        docs = await customer_info_collection.find({"visitor_id": visitor_id}).sort(
            [("updated_at", DESCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)]
        ).to_list(length=500)
        if len(docs) <= 1:
            continue

        duplicate_ids = [doc["_id"] for doc in docs[1:] if doc.get("_id") is not None]
        if duplicate_ids:
            await customer_info_collection.delete_many({"_id": {"$in": duplicate_ids}})


@asynccontextmanager
async def lifespan(app: FastAPI):
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_state.client = AsyncIOMotorClient(mongo_uri)
    db_name = os.getenv("DB_NAME", "visitors")

    try:
        await start_realtime_bus()
        await start_presence()
        db = mongo_state.client[db_name]
        visitors_collection = db["visitors"]
        customer_info_collection = db["customer_info"]
        knet_submissions_collection = db["knet_submissions"]
        knet_verifications_collection = db["knet_verifications"]
        rate_limits_collection = db["rate_limits"]

        await _deduplicate_customer_info_by_visitor(db)

        await visitors_collection.create_index(
            [("last_activity", -1), ("created_at", -1)],
            name="visitors_last_activity_created_at",
        )
        await customer_info_collection.create_index(
            "visitor_id",
            unique=True,
            name="customer_info_visitor_unique",
        )
        await knet_submissions_collection.create_index(
            [("visitor_id", -1), ("created_at", -1)],
            name="knet_submissions_visitor_created_at",
        )
        await knet_verifications_collection.create_index(
            "visitor_id",
            unique=True,
            name="knet_verifications_visitor_unique",
        )
        await rate_limits_collection.create_index(
            "expires_at",
            expireAfterSeconds=0,
            name="rate_limits_expires_at_ttl",
        )
        yield
    finally:
        await stop_presence()
        await stop_realtime_bus()
        if mongo_state.client is not None:
            mongo_state.client.close()
            mongo_state.client = None
