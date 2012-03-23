# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
    Table schema for MySQL and sqlite
"""
from sqlalchemy.ext.declarative import declared_attr, Column
from sqlalchemy import Integer, String, BigInteger, Index


bases = {}


def _add(name, base):
    bases[name] = base


def get_cls(name, base_cls):
    if name in base_cls.metadata.tables:
        return base_cls.metadata.tables[name]

    args = {'__tablename__': name}
    base = bases[name]
    return type(name, (base, base_cls), args).__table__


class _UserNodesBase(object):
    """This table lists all the users associated to a service.

    A user is represented by an email, a uid and its allocated node.
    """
    email = Column(String(255), nullable=False)
    node = Column(String(64), nullable=False)
    service = Column(String(30), nullable=False)
    uid = Column(BigInteger(), primary_key=True, autoincrement=True,
                    nullable=False)

    @declared_attr
    def __table_args__(cls):

        return (Index('userlookup_idx',
                      'email', 'service', unique=True),
                Index('nodelookup_idx',
                      'node', 'service'),
                      {'mysql_engine': 'InnoDB',
                        'mysql_charset': 'utf8',
                        },
                        )

_add('user_nodes', _UserNodesBase)


class _ServicePatternBase(object):
    """ A table that keeps track of the url pattern.
    """
    service = Column(String(30), primary_key=True)
    pattern = Column(String(128), primary_key=True)

_add('service_pattern', _ServicePatternBase)


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

_add('nodes', _NodesBase)
