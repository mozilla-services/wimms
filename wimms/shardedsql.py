# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Sharded version : one DB per service, same DB
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text as sqltext
from sqlalchemy.sql import select, update, and_
from sqlalchemy.exc import OperationalError, TimeoutError

from wimms.sql import (SQLMetadata, _NodesBase, get_user_nodes_table,
                       WRITEABLE_FIELDS)


_GET = sqltext("""\
select
    uid, node
from
    user_nodes
where
    email = :email
""")


_INSERT = sqltext("""\
insert into user_nodes
    (email, node)
values
    (:email, :node)
""")


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
            user_nodes = get_user_nodes_table(engine.driver, Base)

            args = {'__tablename__': 'nodes'}
            nodes = type('Nodes', (_NodesBase, Base), args).__table__

            for table in (nodes, user_nodes):
                table.metadata.bind = engine
                if create_tables:
                    table.create(checkfirst=True)

            self._dbs[service] = engine, nodes, user_nodes

    def _safe_execute(self, service, *args, **kwds):
        """Execute an sqlalchemy query, raise BackendError on failure."""
        engine, __, __ = self._dbs[service]
        try:
            return engine.execute(*args, **kwds)
        except (OperationalError, TimeoutError), exc:
            err = traceback.format_exc()
            logger.error(err)
            raise BackendError(str(exc))

    #
    # Node allocation
    #
    def get_node(self, email, service):
        res = self._safe_execute(service, _GET, email=email)
        res = res.fetchone()
        if res is None:
            return None, None
        return res.uid, res.node

    def allocate_node(self, email, service):
        if self.get_node(email, service) != (None, None):
            raise BackendError("Node already assigned")

        # getting a node
        node = self.get_best_node(service)

        # saving the node
        res = self._safe_execute(service, _INSERT, email=email, node=node)

        # returning the node and last inserted uid
        return res.lastrowid, node

    #
    # Nodes management
    #
    def get_best_node(self, service):
        """Returns the 'least loaded' node currently available, increments the
        active count on that node, and decrements the slots currently available
        """
        __, nodes, __ = self._dbs[service]

        where = [nodes.c.service == service,
                 nodes.c.available > 0,
                 nodes.c.capacity > nodes.c.current_load,
                 nodes.c.downed == 0]

        query = select([nodes]).where(and_(*where))
        query = query.order_by(nodes.c.current_load /
                               nodes.c.capacity).limit(1)
        res = self._safe_execute(service, query)
        res = res.fetchone()
        if res is None:
            # unable to get a node
            raise BackendError('unable to get a node')

        node = str(res.node)
        current_load = int(res.current_load)
        available = int(res.available)
        self.update_node(node, service, available=available - 1,
                         current_load=current_load + 1)
        return res.node

    def update_node(self, node, service, **fields):
        for field in fields:
            if field not in WRITEABLE_FIELDS:
                raise NotImplementedError()

        __, nodes, __ = self._dbs[service]
        where = [nodes.c.service == service, nodes.c.node == node]
        where = and_(*where)
        query = update(nodes, where, fields)
        self._safe_execute(service, query)
        return True
