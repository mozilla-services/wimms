# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Shareded Service Metadata Database.

This implementation provides the same interface as SQLMetadata, but uses
a separate database for each service.  This can help with managing extremely
high load, by keeping the sizes of each table smaller.
"""
from mozsvc.exceptions import BackendError

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import select

from wimms.sql import SQLMetadata

ENGINE_INDEX = 0
SERVICES_INDEX = 1
NODES_INDEX = 2
USERS_INDEX = 3


class ShardedSQLMetadata(SQLMetadata):

    def __init__(self, databases, create_tables=False, pool_size=100,
                 pool_recycle=60, pool_timeout=30, max_overflow=10,
                 pool_reset_on_return='rollback', **kw):

        self._cached_service_ids = {}
        # databases is a string containing one sqluri per service:
        #   service1;sqluri1,service2;sqluri2
        self._dbs = {}
        if pool_reset_on_return.lower() in ('', 'none'):
            pool_reset_on_return = None

        for database in databases.split(','):
            database = database.split(';')
            service, sqluri = (el.strip() for el in database)
            if self._dbkey(service) in self._dbs:
                continue

            Base = declarative_base()
            if sqluri.startswith('mysql') or sqluri.startswith('pymysql'):
                engine = create_engine(
                    sqluri,
                    pool_size=pool_size,
                    pool_recycle=pool_recycle,
                    pool_timeout=pool_timeout,
                    max_overflow=max_overflow,
                    pool_reset_on_return=pool_reset_on_return,
                    logging_name='wimms'
                )
            else:
                # XXX will use a shared pool next
                engine = create_engine(sqluri, poolclass=NullPool)

            engine.echo = kw.get('echo', False)

            self._is_sqlite = (engine.driver == 'pysqlite')
            if self._is_sqlite:
                from wimms.sqliteschemas import get_cls  # NOQA
            else:
                from wimms.schemas import get_cls   # NOQA

            services = get_cls('services', Base)
            nodes = get_cls('nodes', Base)
            users = get_cls('users', Base)

            for table in (services, nodes, users):
                table.metadata.bind = engine
                if create_tables:
                    table.create(checkfirst=True)

            self._dbs[self._dbkey(service)] = (engine, services, nodes, users)

    def _dbkey(self, service):
        """Strip version number, returning just the service name."""
        return service.split('-')[0]

    def _get_engine(self, service=None):
        if service is None:
            raise NotImplementedError()
            return self._dbs.values()[0][ENGINE_INDEX]
        return self._dbs[self._dbkey(service)][ENGINE_INDEX]

    def _get_table(self, service, index):
        return self._dbs[self._dbkey(service)][index]

    def _get_services_table(self, service):
        return self._get_table(service, SERVICES_INDEX)

    def _get_nodes_table(self, service):
        return self._get_table(service, NODES_INDEX)

    def _get_users_table(self, service):
        return self._get_table(service, USERS_INDEX)

    def get_patterns(self):
        """Returns all the service URL patterns."""
        # loop on all the tables to combine the pattern information.
        patterns = []
        for service, elements in self._dbs.items():
            engine = elements[0]
            table = elements[SERVICES_INDEX]
            try:
                res = self._safe_execute(select([table]), engine=engine)
            except BackendError:
                continue
            try:
                for row in res:
                    self._cached_service_ids[row.service] = row.id
                    if row not in patterns:
                        patterns.append(row)
            finally:
                res.close()
        return patterns

    def add_service(self, service, pattern):
        """Add definition for a new service."""
        engine = self._get_engine(service)
        return super(ShardedSQLMetadata, self).add_service(service, pattern,
                                                           engine=engine)
