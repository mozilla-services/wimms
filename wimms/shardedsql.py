# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Sharded version : one DB per service, same DB
"""
from mozsvc.exceptions import BackendError

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from wimms.sql import SQLMetadata


class ShardedSQLMetadata(SQLMetadata):

    def __init__(self, databases, create_tables=False, **kw):
        # databases is a string containing one sqluri per service:
        #   service1;sqluri1,service2;sqluri2
        self._dbs = {}

        for database in databases.split(','):
            database = database.split(';')
            service, sqluri = (el.strip() for el in database)

            if self._dbkey(service) in self._dbs:
                continue

            Base = declarative_base()

            # XXX will use a shared pool next
            engine = create_engine(sqluri, poolclass=NullPool)
            engine.echo = kw.get('echo', False)

            if engine.driver == 'pysqlite':
                from wimms.sqliteschemas import get_cls  # NOQA
            else:
                from wimms.schemas import get_cls   # NOQA

            user_nodes = get_cls('user_nodes', Base)
            nodes = get_cls('nodes', Base)
            patterns = get_cls('service_pattern', Base)

            for table in (nodes, user_nodes, patterns):
                table.metadata.bind = engine
                if create_tables:
                    table.create(checkfirst=True)

            self._dbs[self._dbkey(service)] = (engine, nodes, user_nodes,
                                               patterns)

    def _dbkey(self, service):
        return service.split('-')[0]

    def _get_engine(self, service=None):
        if service is None:
            raise NotImplementedError()
            return self._dbs.values()[0][0]
        return self._dbs[self._dbkey(service)][0]

    def _get_nodes_table(self, service):
        return self._dbs[self._dbkey(service)][1]

    def get_patterns(self):
        """Returns all the service URL patterns."""
        # loop on all the tables to combine the pattern information.
        patterns = []
        for service, elements in self._dbs.items():
            engine = elements[0]
            table = elements[-1]
            try:
                results = self._safe_execute(select([table]), engine=engine)
            except BackendError:
                continue

            for result in results:
                if result not in patterns:
                    patterns.append(result)

        return patterns
