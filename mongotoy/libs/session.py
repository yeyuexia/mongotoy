# coding: utf8

import threading

import pymongo

from .util import get_collection_name
from .queue import flush_queue, get_lock
from .consts import ID, UPDATE, INSERT
from .operators import Set


_lock = threading.Lock()
session = None


class Session(object):
    def __init__(self, **kwargs):
        self._lookup_table = {}
        self.mapper = dict()
        self.session = None
        self.load_table_mapper(kwargs.get("collection_mapper", dict()))

    def load_table_mapper(self, mapper):
        for db, models in mapper.iteritems():
            self.mapper[db] = [get_collection_name(model) for model in models]

    def _search_propable_db(self, collection):
        """
        get propable database by collection
        """
        if collection in self._lookup_table:
            return self._lookup_table[collection]
        for db, collections in self.mapper.iteritems():
            if collection in collections:
                self._lookup_table[collection] = db
                return db
        raise ValueError(
            "not get the collection named %s in collection mapper" % collection
        )

    def execute(self, collection, func, fields):
        db = self._search_propable_db(collection)
        func = getattr(self.session[db][collection], func)
        return func(**fields)

    def close(self):
        """
        try to close session
        """
        try:
            self.session.close()
        except Exception:
            pass


class ReplicaSetSession(Session):

    def __init__(self, db_uri, collection_mapper, **kwargs):
        super(ReplicaSetSession, self).__init__(
            collection_mapper=collection_mapper, **kwargs
        )
        self.session = pymongo.MongoReplicaSetClient(db_uri, **kwargs)


class ClientSession(Session):

    def __init__(self, host, port, max_pool_size, collection_mapper, **kwargs):
        super(ClientSession, self).__init__(
            collection_mapper=collection_mapper, **kwargs
        )
        self.session = pymongo.MongoClient(
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
            host, port, max_pool_size, collection_mapper, **kwargs
        )


def loads_db_mapper(mapper):
    with _lock:
        if not session:
            raise ValueError("session is not created")
    session.load_table_mapper(mapper)


def get_session():
    return session


def _insert(session, models):
    for collection, contexts in models.iteritems():
        commands = [m.to_dict() for m in contexts]
        for command in commands:
            del command[ID]
        ids = session.execute(
            collection, INSERT, dict(doc_or_docs=commands)
        )
        for model, model_id in zip(contexts, ids):
            model._id = model_id


def _update(session, models):
    for model in models:
        update_command = model.to_dict()
        del update_command[ID]
        session.execute(
            model.__collection__, UPDATE,
            dict(
                spec={ID: model._id},
                document=Set(update_command).compile()
            )
        )


def flush():
    """
    push all change to mongo db. It is a block operator so would be takes some time.
    """
    with get_lock():
        session = get_session()
        insert_models = {}
        update_models = []
        for model in flush_queue.get_all():
            if model._id:
                update_models.append(model)
            else:
                collection = model.__collection__
                if collection not in insert_models:
                    insert_models[collection] = []
                insert_models[collection].append(model)
        _insert(session, insert_models)
        _update(session, update_models)
        flush_queue.clear()
