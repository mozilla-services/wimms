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
from sqlalchemy.exc import OperationalError, TimeoutError, IntegrityError

from wimms import logger


_Base = declarative_base()


_GET = sqltext("""\
select
    uid, node, accepted_conditions
from
    user_nodes
where
    email = :email
and
    service = :service
""")


_INSERT = sqltext("""\
insert into user_nodes
    (service, email, node, accepted_conditions)
values
    (:service, :email, :node, :accepted_conditions)
""")


WRITEABLE_FIELDS = ['available', 'current_load', 'capacity', 'downed',
                    'backoff']

_INSERT_METADATA = sqltext("""\
insert into metadata
    (service, name, value, needs_acceptance)
values
    (:service, :name, :value, :needs_acceptance)
""")


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
        self.metadata = get_cls('metadata', _Base)

        for table in (self.user_nodes, self.nodes, self.patterns,
                      self.metadata):
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
        """Return the node of an user for a particular service. If the user
        isn't assigned to a node yet, return None.

        In addition to the node, this method returns the uid of the user and
        a list of urls that the user needs to accept: (uid, node, to_accept)

        - If the user isn't know. idx and node are set to None.
        - If the user is known, idx and node are her id and assigned node.
        - If the user didn't accepted everything, to_accept contains a list of
          urls to read and accept.
        - If the user agreed to the preconditions, to_accept is set to None
        """
        res = self._safe_execute(_GET, email=email, service=service)
        try:
            one = res.fetchone()
            if one is None or one.accepted_conditions == 0:
                to_accept = [i.value for i in self.get_metadata(
                             service, needs_acceptance=True)]

                if not to_accept:
                    to_accept = None
            else:
                to_accept = None

            if one is None:
                return None, None, to_accept
            return one.uid, one.node, to_accept
        finally:
            res.close()

    def allocate_node(self, email, service):
        uid, node, _ = self.get_node(email, service)
        if (uid, node) != (None, None):
            return uid, node

        # getting a node
        node = self.get_best_node(service)

        # saving the node
        try:
            res = self._safe_execute(_INSERT, email=email, service=service,
                                     node=node)
        except IntegrityError:
            uid, node, _ = self.get_node(email, service)
            return uid, node

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

    def _get_metadata_table(self, service):
        return self.metadata

    def _get_nodes_table(self, service):
        return self.nodes

    def _get_user_nodes_table(self, service):
        return self.user_nodes

    def set_metadata(self, service, name, value, needs_acceptance=False):
        def _insert():
            self._safe_execute(_INSERT_METADATA, service=service, name=name,
                               value=value, needs_acceptance=needs_acceptance,
                               close=True)

        def _update():

            where = [metadata.c.service == service, metadata.c.name == name]
            where = and_(*where)

            fields = {'value': value, 'needs_acceptance': needs_acceptance}
            query = update(metadata, where, fields)
            self._safe_execute(query, close=True)

        # first do a request to check if the metadata record exists or not
        metadata = self._get_metadata_table(service)
        where = [metadata.c.service == service, metadata.c.name == name]
        query = select([metadata.c.value]).where(and_(*where))
        res = self._safe_execute(query, close=True)
        if res.fetchone() is None:
            return _insert()
        else:
            return _update()

    def get_metadata(self, service, name=None, needs_acceptance=None):
        metadata = self._get_metadata_table(service)
        where = [metadata.c.service == service]

        if name is not None:
            where.append(metadata.c.name == name)

        if needs_acceptance is not None:
            where.append(metadata.c.needs_acceptance == needs_acceptance)

        fields = [metadata.c.name, metadata.c.value,
                  metadata.c.needs_acceptance]

        query = select(fields).where(and_(*where))
        res = self._safe_execute(query)

        return res.fetchall()

    def set_accepted_conditions_flag(self, service, value, email=None):
        """Update the 'conditions accepted' flag for a service.

        If email is set to None, update the flag for all the users of this
        service.
        """
        user_nodes = self._get_user_nodes_table(service)

        where = [user_nodes.c.service == service, ]
        if email is not None:
            where.append(user_nodes.c.email == email)

        where = and_(*where)

        fields = {'accepted_conditions': value}
        query = update(user_nodes, where, fields)
        self._safe_execute(query, close=True)
