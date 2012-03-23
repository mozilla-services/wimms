# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Sharded version : one DB per service, same DB
"""
import traceback

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text as sqltext
from sqlalchemy.sql import select, update, and_
from sqlalchemy.exc import OperationalError, TimeoutError

from mozsvc.exceptions import BackendError

from wimms import logger
from wimms.sql import SQLMetadata, WRITEABLE_FIELDS


class ShardedSQLMetadata(SQLMetadata):

    def __init__(self, databases, create_tables=False, **kw):
        # databases is a string containing one sqluri per service:
        #   service1;sqluri1,service2;sqluri2
        self._dbs = {}


        for database in databases.split(','):
            database = database.split(';')

            service, sqluri = (el.strip() for el in database)
            Base = declarative_base()

            # XXX will use a shared pool next
            engine = create_engine(sqluri, poolclass=NullPool)
            engine.echo = kw.get('echo', False)

            if engine.driver == 'pysqlite':
                from wimms.sqliteschemas import get_cls
            else:
                from wimms.schemas import get_cls

            user_nodes = get_cls('user_nodes', Base)
            nodes = get_cls('nodes', Base)
            patterns = get_cls('service_pattern', Base)

            for table in (nodes, user_nodes, patterns):
                table.metadata.bind = engine
                if create_tables:
                    table.create(checkfirst=True)

            self._dbs[service] = engine, nodes, user_nodes, patterns

    def _get_engine(self, service=None):
        if service is None:
            raise NotImplementedError()
            return self._dbs.values()[0][0]
        return self._dbs[service][0]

    def _get_nodes_table(self, service):
        return self._dbs[service][1]

    def _get_pattern_table(self, service):
        return self._dbs[service][3]
