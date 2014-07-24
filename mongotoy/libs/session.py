# coding: utf8

import threading

import pymongo

from .base import get_collection_name

_lock = threading.Lock()
session = None


class Session(object):
    def __init__(self, **kwargs):
        self._lookup_table = {}
        self.mapper = kwargs.get("collection_mapper", dict())

    def load_table_mapper(self, mapper):
        for db, models in mapper:
            self.mapper[db] = [get_collection_name(model) for model in models]

    def _search_propable_db(self, collection_name):
        if collection_name in self._lookup_table:
            return self._lookup_table[collection_name]
        for db, collections in self.mapper.iteritems():
            if collection_name in collections:
                self._lookup_table[collection_name] = db
                return db
        raise ValueError(
            "not get the collection named %s in collection mapper" % collection_name
        )

    def execute(self, collection, func, fields):
        db = self._search_propable_db(collection)
        return getattr(self.session[db][collection], func)(**fields)


class ReplicaSetSession(Session):

    def __init__(self, db_uri, **kwargs):
        super(ReplicaSetSession, self).__init__(**kwargs)
        self.session = pymongo.MongoReplicaSetClient(db_uri, **kwargs)


class ClientSession(Session):

    def __init__(self, host, port, max_pool_size, **kwargs):
        super(ClientSession, self).__init__(**kwargs)
        self.session = pymongo.mongo_client.mongo_client(
            host, port, max_pool_size, **kwargs
        )


def create_replica_set_session(db_uri, collection_mapper=None, **kwargs):
    """
    Create a new connection to a MongoDB replica set.
    """
    with _lock:
        global session
        if session:
            session.close()
        session = ReplicaSetSession(db_uri, collection_mapper, **kwargs)


def create_session(host, port=27017, max_pool_size=100,
                   collection_mapper=None, **kwargs):
    """
    Create a new connection to a single MongoDB instance at host:port.
    """
    with _lock:
        global session
        if session:
            session.close()
        session = ClientSession(
            host, port, max_pool_size, collection_mapper=None, **kwargs
        )


def loads_db_mapper(mapper):
    session.load_table_mapper(mapper)
