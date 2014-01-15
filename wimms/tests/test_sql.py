# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest2 import TestCase
import os
import uuid
from wimms.sql import SQLMetadata


TEMP_ID = uuid.uuid4().hex


class NodeAssignmentTests(object):

    backend = None  # subclasses must define this on the instance

    def setUp(self):
        super(NodeAssignmentTests, self).setUp()
        self.backend.add_service('sync-1.0', '{node}/1.0/{uid}')
        self.backend.add_service('sync-1.5', '{node}/1.5/{uid}')
        self.backend.add_service('queuey-1.0', '{node}/{service}/{uid}')
        self.backend.add_node('sync-1.0', 'https://phx12', 100)

    def test_node_allocation(self):
        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertEquals(user, None)

        user = self.backend.create_user("sync-1.0", "tarek@mozilla.com")
        wanted = 'https://phx12'
        self.assertEqual(user['node'], wanted)

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['node'], wanted)

    def test_update_generation_number(self):
        user = self.backend.create_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['generation'], 0)
        self.assertEqual(user['client_state'], '')
        orig_uid = user['uid']
        orig_node = user['node']

        # Changing generation should leave other properties unchanged.
        self.backend.update_user("sync-1.0", user, generation=42)
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 42)
        self.assertEqual(user['client_state'], '')

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 42)
        self.assertEqual(user['client_state'], '')

        # It's not possible to move generation number backwards.
        self.backend.update_user("sync-1.0", user, generation=17)
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 42)
        self.assertEqual(user['client_state'], '')

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 42)
        self.assertEqual(user['client_state'], '')

    def test_update_client_state(self):
        user = self.backend.create_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['generation'], 0)
        self.assertEqual(user['client_state'], '')
        self.assertEqual(set(user['old_client_states']), set(()))
        seen_uids = set((user['uid'],))
        orig_node = user['node']

        # Changing client-state allocates a new userid.
        self.backend.update_user("sync-1.0", user, client_state="aaa")
        self.assertTrue(user['uid'] not in seen_uids)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 0)
        self.assertEqual(user['client_state'], 'aaa')
        self.assertEqual(set(user['old_client_states']), set(("",)))

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertTrue(user['uid'] not in seen_uids)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 0)
        self.assertEqual(user['client_state'], 'aaa')
        self.assertEqual(set(user['old_client_states']), set(("",)))

        seen_uids.add(user['uid'])

        # It's possible to change client-state and generation at once.
        self.backend.update_user("sync-1.0", user,
                                 client_state="bbb", generation=12)
        self.assertTrue(user['uid'] not in seen_uids)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 12)
        self.assertEqual(user['client_state'], 'bbb')
        self.assertEqual(set(user['old_client_states']), set(("", "aaa")))

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertTrue(user['uid'] not in seen_uids)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 12)
        self.assertEqual(user['client_state'], 'bbb')
        self.assertEqual(set(user['old_client_states']), set(("", "aaa")))

        # You can't got back to an old client_state.
        orig_uid = user['uid']
        self.backend.update_user("sync-1.0", user, client_state="aaa")
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 12)
        self.assertEqual(user['client_state'], 'bbb')
        self.assertEqual(set(user['old_client_states']), set(("", "aaa")))


class TestSQLDB(NodeAssignmentTests, TestCase):

    _SQLURI = os.environ.get('WIMMS_SQLURI', 'sqlite:////tmp/wimms.' + TEMP_ID)

    def setUp(self):
        self.backend = SQLMetadata(self._SQLURI, create_tables=True)
        super(TestSQLDB, self).setUp()

    def tearDown(self):
        super(TestSQLDB, self).tearDown()
        if self.backend._engine.driver == 'pysqlite':
            filename = self.backend.sqluri.split('sqlite://')[-1]
            if os.path.exists(filename):
                os.remove(filename)
        else:
            self.backend._safe_execute('drop table services;')
            self.backend._safe_execute('drop table nodes;')
            self.backend._safe_execute('drop table users;')


if os.environ.get('WIMMS_MYSQLURI', None) is not None:
    class TestMySQLDB(TestSQLDB):
        _SQLURI = os.environ.get('WIMMS_MYSQLURI')
