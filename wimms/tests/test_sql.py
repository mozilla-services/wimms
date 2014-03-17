# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
from unittest2 import TestCase
import os
import uuid
import time
from mozsvc.exceptions import BackendError
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

    def test_allocation_to_least_loaded_node(self):
        self.backend.add_node('sync-1.0', 'https://phx13', 100)
        user1 = self.backend.create_user("sync-1.0", "test1@mozilla.com")
        user2 = self.backend.create_user("sync-1.0", "test2@mozilla.com")
        self.assertNotEqual(user1['node'], user2['node'])

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
        with self.assertRaises(BackendError):
            self.backend.update_user("sync-1.0", user, client_state="aaa")

        user = self.backend.get_user("sync-1.0", "tarek@mozilla.com")
        self.assertEqual(user['uid'], orig_uid)
        self.assertEqual(user['node'], orig_node)
        self.assertEqual(user['generation'], 12)
        self.assertEqual(user['client_state'], 'bbb')
        self.assertEqual(set(user['old_client_states']), set(("", "aaa")))

    def test_user_retirement(self):
        self.backend.create_user("sync-1.0", "test@mozilla.com")
        user1 = self.backend.get_user("sync-1.0", "test@mozilla.com")
        self.backend.retire_user("test@mozilla.com")
        user2 = self.backend.get_user("sync-1.0", "test@mozilla.com")
        self.assertTrue(user2["generation"] > user1["generation"])

    def test_cleanup_of_old_records(self):
        service = "sync-1.0"
        # Create 6 user records for the first user.
        # Do a sleep halfway through so we can test use of grace period.
        email1 = "test1@mozilla.com"
        user1 = self.backend.create_user(service, email1)
        self.backend.update_user(service, user1, client_state="a")
        self.backend.update_user(service, user1, client_state="b")
        self.backend.update_user(service, user1, client_state="c")
        break_time = time.time()
        time.sleep(0.1)
        self.backend.update_user(service, user1, client_state="d")
        self.backend.update_user(service, user1, client_state="e")
        records = list(self.backend.get_user_records(service, email1))
        self.assertEqual(len(records), 6)
        # Create 3 user records for the second user.
        email2 = "test2@mozilla.com"
        user2 = self.backend.create_user(service, email2)
        self.backend.update_user(service, user2, client_state="a")
        self.backend.update_user(service, user2, client_state="b")
        records = list(self.backend.get_user_records(service, email2))
        self.assertEqual(len(records), 3)
        # That should be a total of 7 old records.
        old_records = list(self.backend.get_old_user_records(service, 0))
        self.assertEqual(len(old_records), 7)
        # The 'limit' parameter should be respected.
        old_records = list(self.backend.get_old_user_records(service, 0, 2))
        self.assertEqual(len(old_records), 2)
        # The default grace period is too big to pick them up.
        old_records = list(self.backend.get_old_user_records(service))
        self.assertEqual(len(old_records), 0)
        # The grace period can select a subset of the records.
        grace = time.time() - break_time
        old_records = list(self.backend.get_old_user_records(service, grace))
        self.assertEqual(len(old_records), 3)
        # Old records can be successfully deleted:
        for record in old_records:
            self.backend.delete_user_record(service, record.uid)
        old_records = list(self.backend.get_old_user_records(service, 0))
        self.assertEqual(len(old_records), 4)


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
