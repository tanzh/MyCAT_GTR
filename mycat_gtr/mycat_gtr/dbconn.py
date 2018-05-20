#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（数据库连接初始化类模块）
@Usage:
'''

import pymysql
import sys
import log_conf
import execute
import signal
import time


# 获取日志处理器
logger = log_conf.get_logging("dbconn")


class getProcess(object):
    """
    MYSQL数据库对象，负责产生数据库连接 , 此类中的连接采用连接池实现获取连接对象：conn = Process.getConn()
            释放连接对象;conn.close()或del conn
    """
    def __init__(self,Pool):
        # 构造函数，从连接池中取出连接，并生成操作游标
        self._conn = Pool.connection()
        self._cursor = self._conn.cursor()

    def selectAll(self, sql, param=None):
        """
        @summary: 执行查询，并取出所有结果集
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list(字典对象)/boolean 查询到的结果集
        """
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = self._cursor.fetchall()
            else:
                result = False
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result

    def selectOne(self, sql, param=None):
        """
        @summary: 执行查询，并取出第一条
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        注意：这里因为返回只有一行记录，所以返回的是一个字典dict
        """
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = self._cursor.fetchone()
            else:
                result = False
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result

    def selectMany(self, sql, num, param=None):
        """
        @summary: 执行查询，并取出num条结果
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param num:取得的结果条数
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        """
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = self._cursor.fetchmany(num)
            else:
                result = False
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result

    def insertOne(self, sql, param=None):
        """
        @summary: 向数据表插入一条记录
        @param sql:要插入的ＳＱＬ格式
        @param value:要插入的记录数据tuple/list
        @return: insertId 受影响的行数
        """
        logger.debug(sql)
        try:
            if param is None:
                self._cursor.execute(sql)
            else:
                self._cursor.execute(sql,param)
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return self.__getInsertId()

    def insertMany(self, sql, param=None):
        """
        @summary: 向数据表插入多条记录,格式：insert into table (a,b) values (1,1)(2,2)
        @param sql:要插入的ＳＱＬ格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql,param)
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return count

    def __getInsertId(self):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为０
        """
        try:
            self._cursor.execute("SELECT @@IDENTITY AS id")
            result = self._cursor.fetchall()
        except pymysql.Error, e:
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result[0]['id']

    def __query(self, sql, param=None):
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return count

    def update(self, sql, param=None):
        """
        @summary: 更新数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要更新的  值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def delete(self, sql, param=None):
        """
        @summary: 删除数据表记录
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 要删除的条件 值 tuple/list
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def ddl(self, sql, param=None):
        """
        @summary: 执行DDL语句，和update,delete方法无差别，只是为了区分调用
        @param sql: ＳＱＬ格式及条件，使用(%s,%s)
        @param param: 暂未定
        @return: count 受影响的行数
        """
        return self.__query(sql, param)

    def begin(self):
        """
        @summary: 开启事务
        注意：这里一定要用start transaction，不然无法对select语句开启事务
        """
        # self._conn.autocommit(0)
        sql="start transaction"
        logger.debug(sql)
        return self.__query(sql)

    def commit(self):
        """
        @summary: 结束事务
        """
        self._conn.commit()

    def rollback(self):
        """
        @summary: 结束事务
        """
        self._conn.rollback()

    def dispose(self, isEnd=1):
        """
        @summary: 释放连接池资源
        """
        if isEnd == 1:
            self.commit
        else:
            self.rollback
        self._cursor.close()
        self._conn.close()

    def execute(self, sql, param=None):
        """
        @summary: 用于流式游标，执行sql，由fetchone逐行获取
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        注意：这里因为返回只有一行记录，所以返回的是一个字典dict
        """
        logger.debug(sql)
        try:
            if param is None:
                count = self._cursor.execute(sql)
            else:
                count = self._cursor.execute(sql, param)
            if count > 0:
                result = None
            else:
                result = False
        except pymysql.Error, e:
            logger.error(sql)
            if param is not None:
                logger.error(param)
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result

    def fetchmany(self,num):
        """
        @summary: 用于流式游标，逐行获取大表数据
        @param sql:查询ＳＱＬ，如果有查询条件，请只指定条件列表，并将条件值使用参数[param]传递进来
        @param param: 可选参数，条件列表值（元组/列表）
        @return: result list/boolean 查询到的结果集
        注意：这里因为返回只有一行记录，所以返回的是一个字典dict
        """
        try:
            result = self._cursor.fetchmany(num)
        except pymysql.Error, e:
            logger.error("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            self.dispose(0)
            execute.gtr_status = 8  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)
        return result