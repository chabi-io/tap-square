#!/usr/bin/env python

from setuptools import setup

setup(name='tap-square',
      version='3.0.0',
      description='Singer.io tap for extracting data from the Square API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_square'],
      install_requires=[
          'singer-python==5.13.2',
          # New Square SDK (top-level package `square`) - used only for the Timecards
          # (Labor API) stream, which does not exist in the legacy SDK line.
          'squareup==44.1.0.20260520',
          # Legacy Square SDK (top-level package `square_legacy`) - used for all other
          # streams to preserve their existing behavior. Frozen at its only release.
          'squareup_legacy==41.0.0.20250319',
          'backoff==1.10.0',
          'methodtools==0.4.2',
      ],
      extras_require={
          'dev': [
              'ipdb',
              'pylint',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-square=tap_square:main
      ''',
      packages=['tap_square'],
      package_data = {
          'tap_square/schemas': [
              'items.json'
          ],
      },
      include_package_data=True,
)
