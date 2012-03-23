# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Table schema for MySQL and sqlite
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
                              {'mysql_engine': 'InnoDB',
                               'mysql_charset': 'utf8',
                              },
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


