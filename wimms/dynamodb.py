# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
"""DynamoDB backend

"""
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey
from boto.dynamodb2.table import Table

from mozsvc.exceptions import BackendError


def _dynamo_create_tables():


def _dynamo_tables():
    return (
        Table()
    )


class DynamoDB(object):
    """DynamoDB backend for node assignment"""
    def __init__(self, create_tables=False):
        if create_tables:
            _dynamo_create_tables()
        self._users, self._nodes, self._services = _dynamo_user_table()

    def get_user(self, service, email):

    def create_user(self, service, email, generation=0, client_state=''):

    def update_user(self, service, user, generation=None, client_state=None):

    def get_patterns(self):
