# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest2 import TestCase
import os
from wimms.sql import SQLMetadata


class TestSQLDB(TestCase):

    _SQLURI = os.environ.get('WIMMS_SQLURI', 'sqlite:////tmp/wimms')

    def setUp(self):
        super(TestSQLDB, self).setUp()

        self.backend = SQLMetadata(self._SQLURI, create_tables=True)

        # adding a node with 100 slots
        self.backend._safe_execute(
              """insert into nodes (`id`, `node`, `service`, `available`,
                    `capacity`, `current_load`, `downed`, `backoff`)
                values (1, "https://phx12", "sync-1.0", 100, 100, 0, 0, 0)""")

        self._sqlite = self.backend._engine.driver == 'pysqlite'

    def tearDown(self):
        if self._sqlite:
            filename = self.backend.sqluri.split('sqlite://')[-1]
            if os.path.exists(filename):
                os.remove(filename)
        else:
            self.backend._safe_execute('drop table nodes;')
            self.backend._safe_execute('drop table user_nodes;')
            self.backend._safe_execute('drop table metadata;')

    def test_get_node(self):
        unassigned = None, None, None
        self.assertEquals(unassigned,
                  self.backend.get_node("tarek@mozilla.com", "sync-1.0"))

        res = self.backend.allocate_node("tarek@mozilla.com", "sync-1.0")
        wanted = 'https://phx12'
        self.assertEqual(res[1], wanted)

        _, node, _ = self.backend.get_node("tarek@mozilla.com", "sync-1.0")
        self.assertEqual(wanted, node)

        # if we have a node assigned but the terms of services aren't set to
        # true we should return them.
        self.backend.set_metadata('sync-1.0', 'terms-of-service',
                'http://tos', needs_acceptance=True)
        self.backend.set_accepted_conditions_flag('sync-1.0', False)

        _, _, to_accept = self.backend.get_node('tarek@mozilla.com',
                                                'sync-1.0')
        self.assertIn('http://tos', to_accept)

        # if we're not assigned to a node, then we should return the terms of
        # service
        _, _, to_accept = self.backend.get_node('alexis@mozilla.com',
                                                'sync-1.0')
        self.assertIn('http://tos', to_accept)

    def test_metadata(self):
        metadata = (u'terms-of-service', u'http://tos', False)
        self.backend.set_metadata('sync-1.0', 'terms-of-service', metadata[1])
        self.assertIn(metadata,
                  self.backend.get_metadata('sync-1.0', 'terms-of-service'))

        metadata = (u'terms-of-service', u'http://another', False)
        self.backend.set_metadata('sync-1.0', 'terms-of-service', metadata[1])
        self.assertIn(metadata,
                  self.backend.get_metadata('sync-1.0', 'terms-of-service'))

        self.backend.set_metadata('sync-1.0', 'terms-of-service',
                'http://yet-another', needs_acceptance=True)
        self.assertIn(('terms-of-service', 'http://yet-another', True),
            self.backend.get_metadata('sync-1.0', needs_acceptance=True))

    def test_update_metadata(self):
        tos_url = 'http://tos'
        self.backend.set_metadata('sync-1.0', 'terms-of-service', tos_url,
                needs_acceptance=True)
        self.backend.allocate_node('alexis@mozilla.com', 'sync-1.0')
        self.backend.allocate_node('tarek@mozilla.com', 'sync-1.0')

        self.backend.set_accepted_conditions_flag('sync-1.0', False,
                                                  'alexis@mozilla.com')
        _, _, to_accept = self.backend.get_node('alexis@mozilla.com',
                                                'sync-1.0')
        self.assertIn(tos_url, to_accept)


if os.environ.get('WIMMS_MYSQLURI', None) is not None:
    class TestMySQLDB(TestSQLDB):
        _SQLURI = os.environ.get('WIMMS_MYSQLURI')
