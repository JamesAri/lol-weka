import time
import logging

import config


def init_logger():
    # configure root logger
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    filename = config.logging['log_file'].replace('.log', f'_{timestamp}.log')
    logging.basicConfig(
        filename=filename,
        level=logging.NOTSET,
        format="[%(asctime)s]-[%(name)s]-[%(levelname)s]: %(message)s"
    )


__ALL__ = ['init_logger']
