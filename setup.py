# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(here, 'CHANGES.rst')) as f:
    CHANGES = f.read()

requires = ['unittest2', 'mozsvc']


setup(name='wimms',
      version='0.2',
      description='Where Is My Mozilla Service ?',
      long_description=README + '\n\n' + CHANGES,
      classifiers=[
        "Programming Language :: Python",
        ],
      author='Mozilla Services',
      author_email='services-dev@mozilla.org',
      url='https://github.com/mozilla-services/wimms',
      keywords='user database',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=requires,
      test_suite="wimms")
