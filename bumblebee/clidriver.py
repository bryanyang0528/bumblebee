#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import logging

import argparse

from bumblebee import Driver

logger = logging.getLogger(__name__)


def main(argv):
    CLIDriver(argv)


class CLIDriver(object):
    def __init__(self, argv):
        parser = argparse.ArgumentParser()

        if len(argv) == 1:
            parser.print_help()
            sys.exit(1)

        def exit(result):
            sys.exit(1 - result)

        optional = parser._action_groups.pop()
        required = parser.add_argument_group('required arguments')

        required.add_argument('--schema_path',
                              required=True,
                              type=str,
                              metavar='~/path/db.name.json')

        required.add_argument('--target_path',
                              required=True,
                              type=str,
                              metavar='s3://bucket/folder/')

        optional.add_argument('--src_type',
                              required=False,
                              type=str,
                              metavar='hive',
                              default='hive',
                              choices=['hive'])

        optional.add_argument('--schema_mapper',
                              required=False,
                              type=str,
                              metavar='bq',
                              default='bq',
                              choices=['bq'])

        optional.add_argument('--schema_parser',
                              required=False,
                              type=str,
                              metavar='bq',
                              default='bq',
                              choices=['simple', 'bq'])

        optional.add_argument('--target_type',
                              required=False,
                              type=str,
                              metavar='json',
                              default='json',
                              choices=['json', 'csv'])

        optional.add_argument('--condition',
                              required=False,
                              type=str,
                              metavar='dt > "2018-01-01"',
                              default=None)

        parser._action_groups.append(optional)

        parser.set_defaults(
            func=self.run
        )

        args = parser.parse_args(argv[1:])
        inputs = vars(args)
        func = inputs.pop('func')
        exit(func(**inputs))

    @staticmethod
    def run(**kwargs):
        logger.info(kwargs)
        src_type = kwargs.pop('src_type')
        schema_path = kwargs.pop('schema_path')
        schema_mapper = kwargs.pop('schema_mapper')
        schema_parser = kwargs.pop('schema_parser')
        target_type = kwargs.pop('target_type')
        condition = kwargs.pop('condition')

        driver = Driver(src_type, schema_path, schema_mapper, schema_parser)
        valid_df = driver.read(condition=condition).validate().valid_df

        target_path = '{}/{}'.format(kwargs.pop('target_path'), driver.table_name)
        valid_df.write.format(target_type).mode('overwrite').save(target_path)
        valid_df.show()
        logger.info('Success! Please find files in : {}'.format(target_path))
        return True
