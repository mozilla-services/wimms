# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import os

from wimms.shardedsql import ShardedSQLMetadata
from wimms.tests.test_sql import TestSQLDB


_SQLURI = os.environ.get('WIMMS_SQLURI', 'sqlite:////tmp/wimms')
_SQLURI = 'sync-1.0;%s,queuey;%s' % (_SQLURI, _SQLURI)


class TestSQLShardedDB(TestSQLDB):

    def setUp(self):
        super(TestSQLDB, self).setUp()

        self.backend = ShardedSQLMetadata(_SQLURI, create_tables=True)

        # adding a node with 100 slots for sync 1.0
        self.backend._safe_execute(
              """insert into nodes (`node`, `service`, `available`,
                    `capacity`, `current_load`, `downed`, `backoff`)
                values ("https://phx12", "sync-1.0", 100, 100, 0, 0, 0)""",
               service='sync-1.0')

        self._sqlite = self.backend._dbs['sync'][0].driver == 'pysqlite'

    def tearDown(self):
        for service, value in self.backend._dbs.items():
            engine = value[0]
            sqlite = engine.driver == 'pysqlite'
            if sqlite:
                filename = str(engine.url).split('sqlite://')[-1]
                if os.path.exists(filename):
                    os.remove(filename)
            else:
                engine.execute('delete from nodes')
                engine.execute('delete from user_nodes')
