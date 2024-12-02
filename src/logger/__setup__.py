import os
import time
import logging

import config


def init_logger():
    """ Configures root logger """
    os.makedirs(os.path.dirname(config.logging['log_file']), exist_ok=True)

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")

    log_file = config.logging['log_file']
    if log_file.endswith('.log'):
        log_file = log_file[:-4]

    filename = f"{log_file}_{timestamp}.log"

    logging.basicConfig(
        filename=filename,
        level=logging.NOTSET,
        format="[%(asctime)s]-[%(name)s]-[%(levelname)s]: %(message)s"
    )


__ALL__ = ['init_logger']
