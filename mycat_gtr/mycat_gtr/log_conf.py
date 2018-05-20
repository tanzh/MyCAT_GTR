#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（日志模块）
@Usage:
'''

import logging
import logging.handlers
import ConfigParser
import sys

#初始化参数
config = ConfigParser.ConfigParser()
config.read("config.ini")

def get_logging(app_name):
    # 获取日志配置
    log_level = config.get("logging", "log_level")
    if log_level.upper() == 'CRITICAL':
        log_level_int = 50
    elif log_level.upper() == 'ERROR':
        log_level_int = 40
    elif log_level.upper() == 'WARNING':
        log_level_int = 30
    elif log_level.upper() == 'INFO':
        log_level_int = 20
    elif log_level.upper() == 'DEBUG':
        log_level_int = 10
    else:
        log_level_int = 0
    log_name = config.get("logging", "log_name")
    log_when = config.get("logging", "log_when")
    log_interval = config.getint("logging", "log_interval")
    log_count = config.getint("logging", "log_count")
    log_suffix = config.get("logging", "log_suffix")
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # 初始化日志
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level_int)
    formatter = logging.Formatter(log_format)
    file_handler = logging.handlers.TimedRotatingFileHandler(log_name, when=log_when, interval=log_interval,
                                                             backupCount=log_count)
    file_handler.suffix = log_suffix
    file_handler.formatter = formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.formatter = formatter
    logger.addHandler(file_handler)  # 添加文件日志处理器
    logger.addHandler(console_handler)      #添加控制台日志处理器
    return logger