#!/usr/bin/evn python
# encoding: utf-8

# E-mail  :wangs_sdlc@163.com
# Ctime   :2017/09/20

import logging
import os

log_dir = './'

log_formatMap = {
    1: logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
    2: logging.Formatter('%(asctime)s - %(module) : %(lineno)s - %(levelname)s - %(message)s'),
    3: logging.Formatter('%(asctime)s - %(filename)s: %(lineno)s - %(levelname)s - %(message)s'),
    4: logging.Formatter('%(asctime)s - pid:%(thread)s - theadsname:%(threadName)s - %(message)s')
}

log_LevelMap = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}


class Logger():
    def __init__(self, logname='scryapy_1.log', logformat=3, loglevel="debug"):
        """set the defaule logging config"""
        self.name = os.path.join(log_dir, logname)
        self.logformat = logformat
        self.loglevel = log_LevelMap[loglevel.lower()]

        self.logger = logging.getLogger('spider')
        self.logger.setLevel(self.loglevel)

        fh = logging.FileHandler(self.name)
        fh.setLevel(self.loglevel)

        ch = logging.StreamHandler()
        ch.setLevel(self.loglevel)

        formatter = log_formatMap[int(self.logformat)]
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    def getlog(self):
        return self.logger

    def logger_stop(self):
        """shutdown logging process"""
        logging.shutdown()


if __name__ == '__main__':
    # test mode
    log = Logger().getlog()
    log.debug("log test mode")
