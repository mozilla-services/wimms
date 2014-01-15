# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import os
from unittest2 import TestCase

from wimms.shardedsql import ShardedSQLMetadata, ENGINE_INDEX
from wimms.tests.test_sql import NodeAssignmentTests, TEMP_ID


_SQLURI = os.environ.get('WIMMS_SQLURI', 'sqlite:////tmp/wimms.' + TEMP_ID)
_SQLURI = 'sync-1.0;%s,queuey;%s' % (_SQLURI, _SQLURI)


class TestSQLShardedDB(NodeAssignmentTests, TestCase):

    def setUp(self):
        self.backend = ShardedSQLMetadata(_SQLURI, create_tables=True)
        super(TestSQLShardedDB, self).setUp()

    def tearDown(self):
        for service, value in self.backend._dbs.items():
            engine = value[ENGINE_INDEX]
            sqlite = engine.driver == 'pysqlite'
            if sqlite:
                filename = str(engine.url).split('sqlite://')[-1]
                if os.path.exists(filename):
                    os.remove(filename)
            else:
                engine.execute('drop table services')
                engine.execute('drop table nodes')
                engine.execute('drop table users')
