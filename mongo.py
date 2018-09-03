import logging
from motor.motor_asyncio import AsyncIOMotorClient

from opsdroid.database import Database


class DatabaseMongo(Database):
    """A module for opsdroid to allow memory to persist in a mongo database."""

    def __init__(self, config):
        """Start the database connection."""
        logging.debug("Loaded mongo database connector")
        self.name = "mongo"
        self.config = config
        self.client = None
        self.db = None

    async def connect(self, opsdroid):
        """Connect to the database."""
        host = self.config["host"] if "host" in self.config else "localhost"
        port = self.config["port"] if "port" in self.config else "27017"
        database = self.config["database"] \
            if "database" in self.config else "opsdroid"
        path = "mongodb://" + host + ":" + port
        self.client = AsyncIOMotorClient(path)
        self.db = self.client[database]
        logging.info("Connected to mongo")

    async def put(self, collection, data):
        """Insert or replace an object into the given mongo collection."""
        logging.debug("Putting the document into " + collection + " mongo collection")
        if "_id" in data:
            await self.db[collection].update_one({"_id": data["_id"]}, {"$set": data})
        else:
            await self.db[collection].insert_one(data)

    async def get(self, collection, query={}):
        """Get the last document from the given mongo collection."""
        logging.debug("Getting the last inserted document from the " + collection + " mongo collection")
        return await self.db[collection].find_one(
                        {"$query": query, "$orderby": {"$natural" : -1}}
                        )
