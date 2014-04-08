==================
WIMMS Schema Notes
==================

A batch of schema notes during a recent archeoligical code-dig.

Current interface methods are indicated by *, proposed new interface methods
by **.

Primary Sets of Data (Scope)
============================

WIMMS handles several related tasks:

- Allocation of a user to a service based on node-assignment
- Creation of a user for a service
- Lookup of a user record for a service
- Updating the last-seen generation number and/or client-state for a user

Users
=====

Stored uniquely by e-mail address. Server is responsible for determining what
node to store for a user during user creation. 

Multiple copies of the row may exist with differing client states if the user
resets a password

Fields:

- Service
- Email
- Node
- Generation
  Highest timestamp of the browserid cert, based on issuing of cert.
- Client State
  Hash of Sync encryption key.
- Created At
- Replaced At
  When the record was replaced by a new one (during reset).

* Create User
-------------

Creates a new user and allocates a node for the given service in the User
table. Returns the email, uid, node, generation, client state, and old client
states. Given its an initial creation, the old client states will be empty.

* Get User
----------

Get's all the records needed to return the same fields as 'Create User'. If
multiple rows are used, the prior client states (those on a row with a
replaced at) are collapsed into old client states as needed.




Services
========

Lists all the available services along with their endpoint patterns. Service
names are of the form "{app_name}-{app_version}", ie. "sync-1.5". Every service
name must be unique.

Fields:

- Service
- Pattern

Nodes
=====

Tracks all nodes available for each service.

Fields:

- Service
- Node
- Available
  Amount remaining based on special math.
- Current Load
- Capacity
  Total amount of slots allocated.
- Downed
  Whether to assign users to the node or not.
- Backoff
  Not really used or treated the same as downed.

** Allociate node for a given service
-------------------------------------


