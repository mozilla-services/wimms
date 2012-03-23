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
from sqlalchemy.ext.declarative import declarative_base, declared_attr, Column
from sqlalchemy import Integer, String, create_engine, BigInteger, Index
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text as sqltext
from sqlalchemy.exc import OperationalError, TimeoutError

from wimms import logger


_Base = declarative_base()
tables = []


def get_user_nodes_table(driver, base=_Base):
    if 'user_nodes' in _Base.metadata.tables:
        return _Base.metadata.tables['user_nodes']

    if driver != 'pysqlite':
        class UserNodes(base):
            """This table lists all the users associated to a service.

            A user is represented by an email, a uid and its allocated node.
            """
            __tablename__ = 'user_nodes'
            email = Column(String(255), nullable=False)
            node = Column(String(64), nullable=False)
            service = Column(String(30), nullable=False)
            uid = Column(BigInteger(), primary_key=True, autoincrement=True,
                         nullable=False)
            __table_args__ = (Index('userlookup_idx',
                                    'email', 'service', unique=True),
                              Index('nodelookup_idx',
                                    'node', 'service'),
                              {'mysql_engine': 'InnoDB'},
                             )

        return UserNodes.__table__
    else:

        class UserNodes(base):
            """Sqlite version"""
            __tablename__ = 'user_nodes'
            email = Column(String(255))
            node = Column(String(64), nullable=False)
            service = Column(String(30))
            uid = Column(Integer(11), primary_key=True, autoincrement=True)

        return UserNodes.__table__


class _ServicePatternBase(object):
    """ A table that keeps track of the url pattern.
    """
    service = Column(String(30), primary_key=True)
    pattern = Column(String(128), primary_key=True)


class ServicePattern(_ServicePatternBase, _Base):
    __tablename__ = 'service_pattern'


service_pattern = ServicePattern.__table__
tables.append(service_pattern)


class _NodesBase(object):
    """A Table that keep tracks of all nodes per service
    """
    id = Column(BigInteger(), primary_key=True, autoincrement=True,
                nullable=False)
    service = Column(String(30), nullable=False)
    node = Column(String(64), nullable=False)
    available = Column(Integer, default=0, nullable=False)
    current_load = Column(Integer, default=0, nullable=False)
    capacity = Column(Integer, default=0, nullable=False)
    downed = Column(Integer, default=0, nullable=False)
    backoff = Column(Integer, default=0, nullable=False)

    @declared_attr
    def __table_args__(cls):
        return (Index('unique_idx', 'service', 'node', unique=True),
                {'mysql_engine': 'InnoDB'},
               )

class Nodes(_NodesBase, _Base):
    __tablename__ = 'nodes'


nodes = Nodes.__table__
tables.append(nodes)


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

    def __init__(self, sqluri, create_tables=False, **kw):
        self.sqluri = sqluri
        self._engine = create_engine(sqluri, poolclass=NullPool)
        self._engine.echo = kw.get('echo', False)
        self.user_nodes = get_user_nodes_table(self._engine.driver)

        for table in tables + [self.user_nodes]:
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
        res = self._safe_execute(_INSERT, email=email, service=service,
                                 node=node)

        # returning the node and last inserted uid
        return res.lastrowid, node

    #
    # Nodes management
    #
    def get_patterns(self):
        """Returns all the service URL patterns."""
        query = select([self._get_pattern_table()])
        return self._safe_execute(query)

    def _get_nodes_table(self, service):
        return nodes

    def _get_pattern_table(self, service):
        return pattern_table

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
        res = res.fetchone()
        if res is None:
            # unable to get a node
            raise BackendError('unable to get a node')

        node = str(res.node)
        current_load = int(res.current_load)
        available = int(res.available)
        self.update_node(node, service,
                         available=available - 1,
                         current_load=current_load + 1)
        return res.node

    def update_node(self, node, service, **fields):
        nodes = self._get_nodes_table(service)

        for field in fields:
            if field not in WRITEABLE_FIELDS:
                raise NotImplementedError()

        where = [nodes.c.service == service, nodes.c.node == node]
        where = and_(*where)
        query = update(nodes, where, fields)
        self._safe_execute(query)
        return True
