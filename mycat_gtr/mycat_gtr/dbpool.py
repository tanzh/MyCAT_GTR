#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（连接池初始化模块）
@Usage:
'''

from DBUtils.PooledDB import PooledDB
import pymysql

# 获取连接池，默认游标类型DictCursor，因为后面引入了流式游标SSDictCursor
def getPool(host, port, user, password, db, mincached,cursorclass=pymysql.cursors.DictCursor):
    pool = PooledDB(creator=pymysql, mincached=mincached, maxcached=100,
                      host=host, port=port, user=user, passwd=password,
                      db=db, use_unicode=False, charset="utf8", setsession=["set tx_isolation='REPEATABLE-READ'","set innodb_lock_wait_timeout=50"],
                      cursorclass=cursorclass)
    return pool