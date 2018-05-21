from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sys
import logging
from bumblebee.clidriver import main

if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.info(sys.argv)
    main(sys.argv)