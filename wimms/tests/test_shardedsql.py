# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import os

from wimms.shardedsql import ShardedSQLMetadata
from wimms.tests.test_sql import TestSQLDB


_DBS = 'sync;sqlite:////tmp/wimms,queuey;sqlite:////tmp/wimms2'


class TestSQLShardedDB(TestSQLDB):

    def setUp(self):
        super(TestSQLDB, self).setUp()

        self.backend = ShardedSQLMetadata(_DBS, create_tables=True)

        # adding a node with 100 slots for sync
        self.backend._safe_execute('sync',
              """insert into nodes (`node`, `service`, `available`,
                    `capacity`, `current_load`, `downed`, `backoff`)
                  values ("phx12", "sync", 100, 100, 0, 0, 0)""")

        self._sqlite = self.backend._dbs['sync'][0].driver == 'pysqlite'

    def tearDown(self):
        for service, (engine, __, __) in self.backend._dbs.items():

            sqlite = engine.driver == 'pysqlite'
            if sqlite:
                filename = str(engine.url).split('sqlite://')[-1]
                if os.path.exists(filename):
                    os.remove(filename)
            else:
                engine.execute('delete from nodes')
                engine.execute('delete from user_nodes')
