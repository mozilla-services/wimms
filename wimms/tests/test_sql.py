# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest2 import TestCase
import os
from wimms.sql import SQLMetadata


_SQLURI = os.environ.get('WIMMS_SQLURI', 'sqlite:////tmp/wimms')


class TestSQLDB(TestCase):

    def setUp(self):
        super(TestSQLDB, self).setUp()

        self.backend = SQLMetadata(_SQLURI, create_tables=True)

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
            self.backend._safe_execute('delete from nodes')
            self.backend._safe_execute('delete from user_nodes')

    def test_get_node(self):

        unassigned = None, None, None
        self.assertEquals(unassigned,
                          self.backend.get_node("tarek@mozilla.com",
                              "sync-1.0"))

        res = self.backend.allocate_node("tarek@mozilla.com", "sync-1.0")
        wanted = 'https://phx12'
        self.assertEqual(res[1], wanted)
        uid, node, _ = self.backend.get_node("tarek@mozilla.com", "sync-1.0")
        self.assertEqual(wanted, node)

        # if we have a node assigned but the terms of services aren't set, then
        # we should return them.
        self.backend.set_tos('sync-1.0', 'http://new-tos')
        _, _, tos = self.backend.get_node('tarek@mozilla.com', 'sync-1.0')
        self.assertEquals(tos, 'http://new-tos')

        # if we're not assigned to a node, then we should return the terms of
        # service
        _, _, tos = self.backend.get_node('alexis@mozilla.com', 'sync-1.0')
        self.assertEquals(tos, 'http://new-tos')

    def test_metadata(self):
        tos_url = 'http://tos-url'
        self.backend.set_metadata('sync-1.0', 'tos', tos_url)
        self.assertEquals(tos_url,
                          self.backend.get_metadata('sync-1.0', 'tos'))

        new_url = 'http://another-url'
        self.backend.update_metadata('sync-1.0', 'tos', new_url)
        self.assertEquals(new_url,
                          self.backend.get_metadata('sync-1.0', 'tos'))

    def test_update_tos(self):
        tos_url = 'http://tos-url'
        self.backend.set_metadata('sync-1.0', 'tos', tos_url)
        self.backend.allocate_node('alexis@mozilla.com', 'sync-1.0')
        self.backend.allocate_node('tarek@mozilla.com', 'sync-1.0')

        self.backend.set_tos_flag('sync-1.0', False, 'alexis@mozilla.com')
        _, _, tos = self.backend.get_node('alexis@mozilla.com', 'sync-1.0')
        self.assertFalse(tos)

        # the tos flag should be set to false after an update of the tos
        self.backend.set_tos('sync-1.0', 'http://another-url')
        _, _, tos = self.backend.get_node('tarek@mozilla.com', 'sync-1.0')
        self.assertFalse(tos)
        self.assertEquals('http://another-url',
                self.backend.get_metadata('sync-1.0', 'terms-of-service'))
