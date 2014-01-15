# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""
Service Metadata Database.

For each available service, we maintain a list of user accounts and their
associated uid, node-assignemtn and metadata.  We also have a list of nodes
with their load, capacity etc
"""
import time
import traceback
from mozsvc.exceptions import BackendError

from sqlalchemy.sql import select, update, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text as sqltext
from sqlalchemy.exc import OperationalError, TimeoutError, IntegrityError

from wimms import logger


def get_timestamp():
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


_Base = declarative_base()


_GET_USER_RECORDS = sqltext("""\
select
    uid, node, generation, client_state
from
    users
where
    email = :email
and
    service = :service
order by
    created_at desc, uid desc
""")


_CREATE_USER_RECORD = sqltext("""\
insert into
    users
    (service, email, node, generation, client_state, created_at, replaced_at)
values
    (:service, :email, :node, :generation, :client_state, :timestamp, NULL)
""")


_UPDATE_GENERATION_NUMBER = sqltext("""\
update
    users
set
    generation = :generation
where
    service = :service and email = :email and
    generation < :generation and replaced_at is null
""")


_REPLACE_USER_RECORDS = sqltext("""\
update
    users
set
    replaced_at = :timestamp
where
    service = :service and email = :email
    and replaced_at is null and created_at < :timestamp
""")


WRITEABLE_FIELDS = ['available', 'current_load', 'capacity', 'downed',
                    'backoff']


class SQLMetadata(object):

    def __init__(self, sqluri, create_tables=False, pool_size=100,
                 pool_recycle=60, pool_timeout=30, max_overflow=10,
                 pool_reset_on_return='rollback', **kw):
        self._cached_service_ids = {}
        self.sqluri = sqluri
        if pool_reset_on_return.lower() in ('', 'none'):
            pool_reset_on_return = None

        if sqluri.startswith('mysql') or sqluri.startswith('pymysql'):
            self._engine = create_engine(
                sqluri,
                pool_size=pool_size,
                pool_recycle=pool_recycle,
                pool_timeout=pool_timeout,
                pool_reset_on_return=pool_reset_on_return,
                max_overflow=max_overflow,
                logging_name='wimms'
            )
        else:
            self._engine = create_engine(sqluri, poolclass=NullPool)

        self._engine.echo = kw.get('echo', False)

        if self._engine.driver == 'pysqlite':
            from wimms.sqliteschemas import get_cls  # NOQA
        else:
            from wimms.schemas import get_cls  # NOQA

        self.services = get_cls('services', _Base)
        self.nodes = get_cls('nodes', _Base)
        self.users = get_cls('users', _Base)

        for table in (self.services, self.nodes, self.users):
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
            engine = kwds.pop('engine', None)
            if engine is None:
                engine = self._get_engine(kwds.get('service'))

        if 'service' in kwds:
            kwds['service'] = self._get_service_id(kwds['service'])

        try:
            return engine.execute(*args, **kwds)
        except (OperationalError, TimeoutError), exc:
            err = traceback.format_exc()
            logger.error(err)
            raise BackendError(str(exc))

    def get_user(self, service, email):
        params = {'service': service, 'email': email}
        res = self._safe_execute(_GET_USER_RECORDS, **params)
        try:
            row = res.fetchone()
            if row is None:
                return None
            # The first row is the most up-to-date user record.
            user = {
                'email': email,
                'uid': row.uid,
                'node': row.node,
                'generation': row.generation,
                'client_state': row.client_state,
                'old_client_states': {}
            }
            # Any subsequent rows are due to old client-state values.
            row = res.fetchone()
            while row is not None:
                user['old_client_states'][row.client_state] = True
                row = res.fetchone()
            return user
        finally:
            res.close()

    def create_user(self, service, email, generation=0, client_state=''):
        node = self.get_best_node(service)
        params = {
            'service': service, 'email': email, 'node': node,
            'generation': generation, 'client_state': client_state,
            'timestamp': get_timestamp()
        }
        try:
            res = self._safe_execute(_CREATE_USER_RECORD, **params)
        except IntegrityError:
            return self.get_user(service, email)
        else:
            res.close()
            return {
                'email': email,
                'uid': res.lastrowid,
                'node': node,
                'generation': generation,
                'client_state': client_state,
                'old_client_states': {}
            }

    def update_user(self, service, user, generation=None, client_state=None):
        if client_state is None or client_state in user['old_client_states']:
            # uid can stay the same, just update the generation number.
            if generation is not None:
                params = {
                    'service': service,
                    'email': user['email'],
                    'generation': generation
                }
                res = self._safe_execute(_UPDATE_GENERATION_NUMBER, **params)
                res.close()
                user['generation'] = max(generation, user['generation'])
        else:
            # need to create a new record for new client_state.
            if generation is not None:
                generation = max(user['generation'], generation)
            else:
                generation = user['generation']
            now = get_timestamp()
            params = {
                'service': service, 'email': user['email'],
                'node': user['node'], 'timestamp': now,
                'generation': generation, 'client_state': client_state
            }
            try:
                res = self._safe_execute(_CREATE_USER_RECORD, **params)
            except IntegrityError:
                user.update(self.get_user(service, user['email']))
            else:
                self.get_user(service, user['email'])
                user['uid'] = res.lastrowid
                user['generation'] = generation
                user['old_client_states'][user['client_state']] = True
                user['client_state'] = client_state
                res.close()
            # mark old records as having been replaced.
            # if we crash here, they are unmarked and we may fail to
            # garbage collect them for a while, but the active state
            # will be undamaged.
            params = {
                'service': service, 'email': user['email'], 'timestamp': now
            }
            res = self._safe_execute(_REPLACE_USER_RECORDS, **params)
            res.close()

    #
    # Nodes management
    #

    def _get_service_id(self, service):
        try:
            return self._cached_service_ids[service]
        except KeyError:
            services = self._get_services_table(service)
            query = select([services.c.id])
            query = query.where(services.c.service == service)
            res = self._safe_execute(query)
            row = res.fetchone()
            res.close()
            if row is None:
                raise BackendError('unknown service: ' + service)
            self._cached_service_ids[service] = row.id
            return row.id

    def get_patterns(self):
        """Returns all the service URL patterns."""
        query = select([self.services])
        res = self._safe_execute(query)
        patterns = list(res.fetchall())
        for row in patterns:
            self._cached_service_ids[row.service] = row.id
        res.close()
        return patterns

    def add_service(self, service, pattern, **kwds):
        """Add definition for a new service."""
        res = self._safe_execute("""
          insert into services (service, pattern)
          values (:servicename, :pattern)
        """, servicename=service, pattern=pattern, **kwds)
        res.close()
        return res.lastrowid

    def add_node(self, service, node, capacity, **kwds):
        """Add definition for a new node."""
        res = self._safe_execute(
            """
            insert into nodes (id, service, node, available, capacity,
                               current_load, downed, backoff)
            values (NULL, :service, :node, :available, :capacity,
                    :current_load, :downed, :backoff)
            """,
            service=service, node=node, capacity=capacity,
            available=kwds.get('available', capacity),
            current_load=kwds.get('current_load', 0),
            downed=kwds.get('downed', 0),
            backoff=kwds.get('backoff', 0),
        )
        res.close()

    def get_best_node(self, service):
        """Returns the 'least loaded' node currently available, increments the
        active count on that node, and decrements the slots currently available
        """
        nodes = self._get_nodes_table(service)

        where = [nodes.c.service == self._get_service_id(service),
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

    def _get_services_table(self, service):
        return self.services

    def _get_nodes_table(self, service):
        return self.nodes

    def _get_users_table(self, service):
        return self.users
