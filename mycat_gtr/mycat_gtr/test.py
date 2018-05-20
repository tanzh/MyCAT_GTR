#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（测试模块）
@Usage:
'''
import time
from DBUtils.PooledDB import PooledDB
import pymysql
from multiprocessing import Pool,Process
import multiprocessing
import os
import types

types.LongType

mysql_user="root"
mysql_password="root"
mysql_port=3306
mysql_host="172.17.210.8"
mysql_db="db2"


def abc():
    print u"哈哈哈，我是一个测试模块"

abc()
