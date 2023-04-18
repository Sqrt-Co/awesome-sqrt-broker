import logging


def get_upbit_broker_logger():
    logger = logging.getLogger("upbit_broker")

    if len(logger.handlers) == 0:
        logger.setLevel(logging.DEBUG)

        # add handler
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s|%(filename)s-%(funcName)s:%(lineno)s] >> %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        stream_handler = logging.StreamHandler()
        timed_rotating_handler = logging.handlers.TimedRotatingFileHandler(
            "/live/share/log/upbit_broker/upbit_broker.log",
            when="D",
            interval=1,
            utc=True,
        )

        stream_handler.setFormatter(formatter)
        timed_rotating_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(timed_rotating_handler)

    return logger
