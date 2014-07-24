# coding: utf8

import pymongo


class Session(object):
    def __init__(self, **kwargs):
        self._lookup_table = kwargs.get("db_config", dict())

    def load_db_tables(self, configs):
        self.configs = configs

    def _search_propable_db(self, table_name):
        if table_name in self._lookup_table:
            return self._lookup_table[table_name]
        for db, tables in self.configs.iteritems():
            if table_name in tables:
                self._lookup_table[table_name] = db
                return db

    def execute(self, table, func, fields):
        db = self._search_propable_db(table)
        return getattr(self.session[db][table], func)(**fields)


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
