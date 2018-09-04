import logging
from pymongo import MongoClient

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
        self.client = MongoClient(path)
        self.db = self.client[database]
        logging.info("Connected to mongo")

    async def put(self, key, data):
        """Insert or replace an object into the given mongo collection (key)."""
        logging.debug("Putting the document into " + key + " mongo collection")
        if "_id" in data:
            await self.db[key].update_one({"_id": data["_id"]}, {"$set": data})
        else:
            await self.db[key].insert_one(data)

    async def get(self, key):
        """Get a document from the given mongo collection."""
        # hack parsing a single string key into the mongo collection name and search query parameters
        collection, field, value = key.split('/')
        logging.debug("Getting the last inserted document from the " + collection + " mongo collection")
        return self.db[collection].find_one(
                        {"$query": {field: value}, "$orderby": {"$natural" : -1}}
                        )
