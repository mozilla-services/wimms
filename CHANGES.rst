CHANGES
=======

XXXX - 0.4
----------

- Removed "service acceptance" flags and the related metadata table.
- Added tracking of optional "generation number" for each user account,
  which can be used to more quickly detect password change events.
- Added tracking of optional "client state" string for each user account,
  which can be used to force node reallocation when client state changes.

2012-07-24 - 0.3
----------------

- Added a metadata table which is able to store information related to the user
  and service.

2012-07-18 - 0.2.1
------------------

- allocated_node will just return an existing node.


2012-04-18 - 0.2
----------------

- added the pool_reset_on_return option


2012-04-17 - 0.1
----------------

- initial release
