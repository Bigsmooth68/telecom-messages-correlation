import logging, sys

class custom_logger(logging.Logger):

    def __init__(self, name, level=logging.INFO):
        super(custom_logger, self).__init__(name, level)
        handler = logging.StreamHandler(sys.stdout)
        fmt = '%(asctime)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(fmt)
        handler.setFormatter(formatter)
        self.addHandler(handler)
