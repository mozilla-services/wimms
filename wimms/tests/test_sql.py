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

        unassigned = None, None
        self.assertEquals(unassigned,
                          self.backend.get_node("tarek@mozilla.com",
                              "sync-1.0"))

        res = self.backend.allocate_node("tarek@mozilla.com", "sync-1.0")
        wanted = 'https://phx12'
        self.assertEqual(res[1], wanted)
        uid, node = self.backend.get_node("tarek@mozilla.com", "sync-1.0")
        self.assertEqual(wanted, node)
