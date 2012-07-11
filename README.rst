=====
WIMMS
=====

**WIMMS** stands for *Where Is My Mozilla Service ?*.

This library implements a SQL backend for a database that
lists for a list of service, (user, node, service) tuples.

It's used by Mozilla's Token Server.

To see where WIMMS fits in the whole architecture, see:

.. image:: http://ziade.org/token-org.png

Tests
-----

To run the tests for mysql, you need to install a mysqlserver and export the 
'WIMMS_MYSQLURI' variable properly. Something like this::

    $ export WIMMS_MYSQLURI="mysql://user:pass@host/db"
