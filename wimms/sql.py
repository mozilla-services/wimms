# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Metadata Database

    Contains for each user a list of node/uid/service
    Contains a list of nodes and their load, capacity etc
"""
import traceback
from mozsvc.exceptions import BackendError

from sqlalchemy.sql import select, update, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text as sqltext
from sqlalchemy.exc import OperationalError, TimeoutError

from wimms import logger


_Base = declarative_base()


_GET = sqltext("""\
select
    uid, node
from
    user_nodes
where
    email = :email
and
    service = :service
""")


_INSERT = sqltext("""\
insert into user_nodes
    (service, email, node)
values
    (:service, :email, :node)
""")


WRITEABLE_FIELDS = ['available', 'current_load', 'capacity', 'downed',
                    'backoff']


class SQLMetadata(object):

    def __init__(self, sqluri, create_tables=False, pool_size=100,
                 pool_recycle=60, pool_timeout=30, max_overflow=10,
                 pool_reset_on_return='rollback', **kw):
        self.sqluri = sqluri
        if pool_reset_on_return.lower() in ('', 'none'):
            pool_reset_on_return = None

        if (self.sqluri.startswith('mysql') or
            self.sqluri.startswith('pymysql')):
            self._engine = create_engine(sqluri,
                                    pool_size=pool_size,
                                    pool_recycle=pool_recycle,
                                    pool_timeout=pool_timeout,
                                    pool_reset_on_return=pool_reset_on_return,
                                    max_overflow=max_overflow,
                                    logging_name='wimms')

        else:
            self._engine = create_engine(sqluri, poolclass=NullPool)

        self._engine.echo = kw.get('echo', False)

        if self._engine.driver == 'pysqlite':
            from wimms.sqliteschemas import get_cls  # NOQA
        else:
            from wimms.schemas import get_cls  # NOQA

        self.user_nodes = get_cls('user_nodes', _Base)
        self.nodes = get_cls('nodes', _Base)
        self.patterns = get_cls('service_pattern', _Base)

        for table in [self.user_nodes, self.nodes, self.patterns]:
            table.metadata.bind = self._engine
            if create_tables:
                table.create(checkfirst=True)

    def _get_engine(self, service=None):
        return self._engine

    def _safe_execute(self, *args, **kwds):
        """Execute an sqlalchemy query, raise BackendError on failure."""
        if hasattr(args[0], 'bind'):
            engine = args[0].bind
        else:
            engine = None

        if engine is None:
            engine = kwds.get('engine')
            if engine is None:
                engine = self._get_engine(kwds.get('service'))
            else:
                del kwds['engine']

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
        res = self._safe_execute(_GET, email=email, service=service)
        try:
            one = res.fetchone()
            if one is None:
                return None, None
            return one.uid, one.node
        finally:
            res.close()

    def allocate_node(self, email, service):
        if self.get_node(email, service) != (None, None):
            raise BackendError("Node already assigned")

        # getting a node
        node = self.get_best_node(service)

        # saving the node
        res = self._safe_execute(_INSERT, email=email, service=service,
                                 node=node)
        lastrowid = res.lastrowid
        res.close()

        # returning the node and last inserted uid
        return lastrowid, node

    #
    # Nodes management
    #
    def get_patterns(self):
        """Returns all the service URL patterns."""
        query = select([self.patterns])
        res = self._safe_execute(query)
        patterns = res.fetchall()
        res.close()
        return patterns

    def _get_nodes_table(self, service):
        return self.nodes

    def get_best_node(self, service):
        """Returns the 'least loaded' node currently available, increments the
        active count on that node, and decrements the slots currently available
        """
        nodes = self._get_nodes_table(service)

        where = [nodes.c.service == service,
                 nodes.c.available > 0,
                 nodes.c.capacity > nodes.c.current_load,
                 nodes.c.downed == 0]

        query = select([nodes]).where(and_(*where))
        query = query.order_by(nodes.c.current_load /
                               nodes.c.capacity).limit(1)
        res = self._safe_execute(query)
        one = res.fetchone()
        if one is None:
            # unable to get a node
            res.close()
            raise BackendError('unable to get a node')

        node = str(one.node)
        res.close()

        # updating the table
        where = [nodes.c.service == service, nodes.c.node == node]
        where = and_(*where)
        fields = {'available': nodes.c.available - 1,
                  'current_load': nodes.c.current_load + 1}
        query = update(nodes, where, fields)
        con = self._safe_execute(query, close=True)
        con.close()

        return node
