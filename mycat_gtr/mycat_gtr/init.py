#！coding=utf-8
'''
@Author: tanzh
@Date: 20180120
@Description: mycat全局表不一致修复（创建表结构）
@Usage:
表结构说明
global_table_id       #存储全局表id，数据为【临时】存储，checksum模块校验完后会被清空
global_table_chunk      #把global_table_id分成多个chunk去检查，检查结果存放到该表，数据为【临时】存储，checksum模块校验完后会被清空
global_table_chunk_differ   #有差异的chunk存放到该表，数据为【临时】存储，checksum模块校验完后会被清空
global_table_status   #记录程序运行记录和状态，数据为【永久性】存储，校验结束不会被清空
global_table_id_differ  #对有差异的chunk根据id进行检查，差异id存放到该表，数据为【永久性】存储，校验结束不会被清空
bak_table_name    #修复前的备份表，数据为【永久性】存储，校验结束不会被清空
'''


from __future__ import division      #解决per = 100/(count /2000)没有小数的问题
import re
import dbconn
import commands
import log_conf
import sys
import os
import execute
import signal
import time

#获取日志处理器
logger = log_conf.get_logging("init")


def check(global_table,node_pool_list,local_mysql_pool,mysql_db,time_column,timr_format,run_mode):
    #判断是否有其他gtr在使用当前mysql_db
    local_mysql_process = dbconn.getProcess(local_mysql_pool)  # mysql线程只需要一个
    sql="select * from information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='global_table_status';" % mysql_db
    sql_result = local_mysql_process.selectAll(sql)
    if sql_result:
        sql = "select id,global_table,pid from global_table_status where status=0"
        sql_result = local_mysql_process.selectAll(sql)
        if sql_result:
            for row in sql_result:
                pid_count = commands.getstatusoutput("ps -ef | grep -w %d | grep -v grep | wc -l" % row["pid"])[1]
                if pid_count != "0":
                    #如果不等于0，说明存在进程，当前mysql_db则不能使用，避免影响正在校验的数据
                    logger.error(u"【%s】全局表校验正在使用db【%s】，请重新配置mysql_db" % (row["global_table"],mysql_db))
                    sys.exit(0)
                else:
                    #如果不存在进程，可能上次是异常退出，所以把状态更新为2
                    sql = "update global_table_status set status=2 where id=%d" % row["id"]
                    local_mysql_process.update(sql)
                    local_mysql_process.commit()

    #检查表结构是否一致
    last_table_info = None
    this_table_info = None
    for row in node_pool_list:
        sql = "show create table %s" % global_table
        node_process = dbconn.getProcess(row["pool"])
        sql_result = node_process.selectAll(sql)
        """
        先从结果做判断，如果某个节点没有结果，则认为不一致
       """
        if not sql_result:
            logger.error(u"节点%s不存在表%s" % (row["database"],global_table))
            sys.exit(0)
        """
        都有结果了，则从获取的结果判断，上下判断，如果存在不一致就记录
       """
        if last_table_info is not None:
            # 如果上一个节点表结构不为空，则说明这不是第一个分片，需要和当前chunk比较
            this_table_info = sql_result[0]["Create Table"]
            # 去掉主键AUTO_INCREMENT和AUTO_INCREMENT=40510
            this_table_info = re.sub(r"\bAUTO_INCREMENT\S*?\s\b", "", this_table_info, count=2)
            if this_table_info != last_table_info:
                logger.error(u"节点%s表结构不一致" % row["database"])
                sys.exit(0)
            else:
                #如果一样，则赋值给last_table_info
                last_table_info=this_table_info
        else:
            # 如果上一个节点表结构为空，则说明是第一个db分片，需要赋值给last，用于和下一个对比
            last_table_info = sql_result[0]["Create Table"]
            # 去掉主键AUTO_INCREMENT和AUTO_INCREMENT=40510
            last_table_info = re.sub(r"\bAUTO_INCREMENT\S*?\s\b", "", last_table_info, count=2)
        node_process.dispose()

    #获取time_value
    time_value = None
    if run_mode in (0, 2):
        #需要循环所有节点，因为可能某个节点没有数据
        for row in node_pool_list:
            node_process = dbconn.getProcess(row["pool"])
            sql="select * from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME='%s'" % (row["database"],global_table,time_column)
            sql_result = node_process.selectAll(sql)
            if sql_result:
                sql="select %s from %s limit 1;" % (time_column,global_table)
                sql_result = node_process.selectAll(sql)
                if sql_result:
                    sql_result=sql_result[0][time_column]
                    if timr_format == 'str':
                        #"%s占位符是sql_result[0][time_column]
                        #"转换成元组：time.strptime(str(sql_result[0][time_column]), '%Y-%m-%d %H:%M:%S')
                        #"转换成时间戳：time.mktime(time.strptime(str(sql_result[0][time_column]), '%Y-%m-%d %H:%M:%S'))
                        try:
                            time.mktime(time.strptime(str(sql_result), '%Y-%m-%d %H:%M:%S'))
                        except:
                            logger.error(u"%s字段不是YYYY-MM-DD HH:MM:SS格式或者该字段存在空值" % time_column)
                            sys.exit(0)
                        time_value="time.mktime(time.strptime(str('%s'),'%%Y-%%m-%%d %%H:%%M:%%S'))"
                    elif timr_format == 'int':
                        try:
                            int(sql_result)
                        except:
                            logger.error(u"%s字段不是整型时间戳格式或者该字段存在空值" % time_column)
                            sys.exit(0)
                        time_value="'%s'"
                    break
            else:
                logger.error(u"time_column配置错误，不存在字段%s" % time_column)
                sys.exit(0)
            node_process.dispose()
        #如果所有都没有数据，则不存在数据
        if not sql_result:
            logger.error(u"%s不存在数据，请检查" % global_table)
            sys.exit(0)

    node_process = dbconn.getProcess(node_pool_list[0]["pool"])  #只需要一个节点的
    # 检查是否存在主键
    primary_key_list = []
    sql = "select COLUMN_NAME from information_schema.key_column_usage t1,information_schema.table_constraints t2 where t1.TABLE_SCHEMA=t2.TABLE_SCHEMA and t1.TABLE_NAME=t2.TABLE_NAME and t1.CONSTRAINT_NAME=t2.CONSTRAINT_NAME and t2.CONSTRAINT_TYPE='PRIMARY KEY' and t2.TABLE_SCHEMA='%s' and t2.TABLE_NAME='%s'" % (node_pool_list[0]["database"], global_table)
    sql_result = node_process.selectAll(sql)
    if sql_result:
        for row in sql_result:
            primary_key_list.append(row["COLUMN_NAME"])
        if len(primary_key_list) > 1:
            logger.error(u"全局表%s主键超过一个字段，不执行检查" % global_table)
            sys.exit(0)
        else:
            primary_key_name=primary_key_list[0]
    else:
        logger.error(u"全局表%s不存在主键，不执行检查" % global_table)
        sys.exit(0)
    #查找主键字段类型
    sql = "desc %s" % global_table
    sql_result = node_process.selectAll(sql)
    for row in sql_result:
        if row["Field"] == primary_key_list[0]:
            primary_key_type = row["Type"]

    sql="select * from information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='global_table_id';" % mysql_db
    sql_result = local_mysql_process.selectAll(sql)
    if sql_result:
        sql = "desc global_table_id"
        sql_result = local_mysql_process.selectAll(sql)
        for row in sql_result:
            if row["Field"] == "global_id":
                global_id_type = row["Type"]
        if primary_key_type != global_id_type:
            logger.error(u"全局表%s的主键类型和上次修复表的主键类型不一样，请重新配置mysql_db" % global_table)
            sys.exit(0)

    unique_column_list = []
    if run_mode in (0,2):
        # 判断表结构是否存在唯一索引，多个唯一索引则返回
        unique_count = None
        sql = "select count(*) as count from information_schema.table_constraints where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and CONSTRAINT_TYPE='UNIQUE'" % (node_pool_list[0]["database"], global_table)
        sql_result = node_process.selectAll(sql)
        unique_count = sql_result[0]["count"]
        if unique_count > 1:
            # 如果结果大于1，说明有多个唯一索引，不执行恢复，直接退出
            logger.error(u"全局表%s存在多个唯一索引，不执行修复，请用模式1（只检查）运行" % global_table)
            sys.exit(0)
        elif unique_count == 1:
            logger.info(u"全局表%s存在一个唯一索引" % global_table)
            # 存在一个唯一索引，查找唯一索引字段
            sql = "select COLUMN_NAME from information_schema.key_column_usage t1,information_schema.table_constraints t2 where t1.TABLE_SCHEMA=t2.TABLE_SCHEMA and t1.TABLE_NAME=t2.TABLE_NAME and t1.CONSTRAINT_NAME=t2.CONSTRAINT_NAME and t2.CONSTRAINT_TYPE='UNIQUE' and t2.TABLE_SCHEMA='%s' and t2.TABLE_NAME='%s'" % (node_pool_list[0]["database"], global_table)
            sql_result = node_process.selectAll(sql)
            for row in sql_result:
                unique_column_list.append(row["COLUMN_NAME"])
        else:
            logger.info(u"全局表%s不存在唯一索引" % global_table)
    local_mysql_process.dispose()
    node_process.dispose()
    return  primary_key_name,primary_key_type,time_value,unique_column_list

def create_local_table(global_table,bak_global_table,node_pool_list,local_mysql_pool,mysql_db,primary_key_type):
    local_mysql_process = dbconn.getProcess(local_mysql_pool)  # mysql线程只需要一个
    node_process = dbconn.getProcess(node_pool_list[0]["pool"])  #只需要一个节点的

    #创建表结构
    sql = "create table if not exists global_table_id( \
        id int primary key AUTO_INCREMENT comment '主键',\
        global_id %s comment '业务表id',\
        status tinyint default 0 not null comment '数据处理状态：0-刚添加，1-准备checksum，2-正在校验checksum，3-checksum结果一致，4-正在修复，5-修复完成，-2-校验checksum失败，-4-正在修复',\
        create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\
        update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\
        unique udx_global_id(global_id)\
    ) comment '从所有节点去重后的ID，数据来自多个global_table_id_model';\
    CREATE TABLE if not exists global_table_chunk (\
      id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',\
      db char(64) NOT NULL COMMENT '库名',\
      global_table varchar(64) NOT NULL COMMENT '全局表名',\
      chunk int(11) NOT NULL COMMENT '第几个chunk',\
      chunk_time float DEFAULT NULL COMMENT '当前块检查耗时',\
      chunk_index varchar(200) DEFAULT 'primary key' COMMENT 'chunk走的索引',\
      upper_boundary %s COMMENT '上界限，chunk_index的值，比如id=5',\
      lower_boundary %s COMMENT '下界限，chunk_index的值，比如id=10',\
      crc char(40) NOT NULL COMMENT '当前chunk的checksum',\
      cnt int(11) NOT NULL COMMENT '当前chunk的行数',\
      create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\
      update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\
      PRIMARY KEY (id),\
      UNIQUE KEY db (db,global_table,chunk),\
      KEY idx_chunk (chunk)\
    ) ENGINE=InnoDB AUTO_INCREMENT=1183 DEFAULT CHARSET=utf8 COMMENT='记录所有db的chunk，数据来自对global_table_id的分析';\
    CREATE TABLE if not exists global_table_chunk_differ (\
      id int primary key AUTO_INCREMENT comment '主键',\
      global_table varchar(64) NOT NULL comment '全局表名',\
      chunk int(11) NOT NULL comment '第几个chunk',\
      upper_boundary %s comment '上界限，chunk_index的值，比如id=5',\
      lower_boundary %s comment '下界限，chunk_index的值，比如id=10',\
      create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\
      update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\
      unique KEY (global_table,chunk),\
      KEY ts_db_tbl (update_time,global_table)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8 comment '存在差异的chunk，数据来自对global_table_chunk的分析结果';\
    create table if not exists global_table_id_differ(\
        id int primary key AUTO_INCREMENT comment '主键',\
        global_table varchar(64) NOT NULL COMMENT '全局表名',\
        global_id %s comment '业务表id',\
        status tinyint not null default 0 comment '状态：0-刚添加或者未修复，1-已修复，2-记录已经物理删除，3-记录加锁失败，4-记录修复失败，5-无法自动修复',\
        differ_type tinyint not null default 0 comment '差异类型：0-初始值，1-某节点丢失数据，2-字段值不一致，3-一个id对应多个唯一索引值',\
        create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\
        update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\
        key idx_global_id(global_id)\
    ) comment '存在差异的ID，数据来自对global_table_chunk_differ的分析';\
    create table if not exists global_table_status(\
        id int primary key AUTO_INCREMENT comment '主键',\
        global_table varchar(64) NOT NULL comment '全局表名',\
        status tinyint not null default 0 comment '运行状态：0-开始运行，1-正常退出，2-异常退出（kill或ctrl+c退出就更新，崩溃下次执行才更新），3-表结构存在异常(暂时无用到)，4-全局表不存在数据，5-模式3没有找到global_id，6-sync全局表存在多个唯一索引(暂时无用到)，7-sync差异数量超过auto_repair_size不执行修复，8-sql执行错误',\
        pid int not null default 0 comment '记录mycat_gtr进程pid',\
        run_mode tinyint not null default 0 comment '运行模式：0-检查并修复，1-只检查，2-只修复，3-断电续检查' ,\
        run_time int DEFAULT null comment '校验总耗时，单位分钟',\
        create_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',\
        update_time datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',\
        KEY idx_global_table_pid (global_table,pid)\
    ) comment '全局表校验记录和当前状态';" % (primary_key_type,primary_key_type,primary_key_type,primary_key_type,primary_key_type,primary_key_type)
    local_mysql_process.ddl(sql)

    # 创建备份表
    bak_global_table_tmp=bak_global_table+"_tmp"
    sql = "show create table %s" % global_table
    sql_result = node_process.selectAll(sql)[0]["Create Table"]
    if sql_result:
        create_bak_table_info = re.sub(global_table, bak_global_table_tmp, sql_result, count=1)
        create_bak_table_info = re.sub("CREATE TABLE", "CREATE TABLE if not exists", create_bak_table_info, count=1)
        #去掉主键AUTO_INCREMENT和AUTO_INCREMENT=40510
        create_bak_table_info = re.sub(r"\bAUTO_INCREMENT\S*?\s\b", "", create_bak_table_info, count=2)
        local_mysql_process.ddl(create_bak_table_info)
    else:
        logger.error(u"节点%s不存在表%s" % (node_pool_list[0]["database"], global_table))
        sys.exit(0)

    #给备份表添加字段gtr_source_db，gtr_differ_id，gtr_repair_standard
    sql = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_SCHEMA='%s' and TABLE_NAME='%s' and COLUMN_NAME in ('gtr_source_db','gtr_differ_id','gtr_repair_standard')" % (mysql_db, bak_global_table_tmp)
    sql_result = local_mysql_process.selectAll(sql)
    column_name_list=[]
    if sql_result:
        for row in sql_result:
            column_name_list.append(row["COLUMN_NAME"])
    if 'gtr_source_db' not in column_name_list:
        sql = "alter table %s add gtr_source_db varchar(30) comment '来源db'" % bak_global_table_tmp
        local_mysql_process.ddl(sql)
    if 'gtr_repair_standard' not in column_name_list:
        sql = "alter table %s add gtr_repair_standard tinyint not null default 0 comment '修复标准：1-以这条记录为修复标准' after gtr_source_db" % bak_global_table_tmp
        local_mysql_process.ddl(sql)
    if 'gtr_differ_id' not in column_name_list:
        sql = "alter table %s add gtr_differ_id int comment '关联global_table_id_differ的id' after gtr_repair_standard" % bak_global_table_tmp
        local_mysql_process.ddl(sql)

    #给备份表删除主键和索引，添加idx_gtr_differ_id索引
    sql = "select distinct INDEX_NAME from information_schema.STATISTICS where TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (mysql_db,bak_global_table_tmp)
    sql_result = local_mysql_process.selectAll(sql)
    index_name_list=[]
    if sql_result:
        for row in sql_result:
            index_name_list.append(row["INDEX_NAME"])
    for row in index_name_list:
        if row == "PRIMARY":
            sql = "alter table %s drop primary key" % bak_global_table_tmp
            local_mysql_process.ddl(sql)
        elif row != 'idx_gtr_differ_id':
            sql = "alter table %s drop index %s" % (bak_global_table_tmp,row)
            local_mysql_process.ddl(sql)
    if 'idx_gtr_differ_id' not in index_name_list:
        sql = "alter table %s add index idx_gtr_differ_id(gtr_differ_id)" % bak_global_table_tmp
        local_mysql_process.ddl(sql)

    #判断备份表结构是否一致
    sql = "show create table %s" % bak_global_table_tmp
    sql_result = local_mysql_process.selectAll(sql)
    if sql_result:
        bak_global_table_tmp_info = sql_result[0]["Create Table"]
        # 去掉主键AUTO_INCREMENT和AUTO_INCREMENT=40510
        bak_global_table_tmp_info = re.sub(r"\bAUTO_INCREMENT\S*?\s\b", "", bak_global_table_tmp_info, count=2)
        # 去掉表名
        bak_global_table_tmp_info = re.sub(bak_global_table_tmp, "", bak_global_table_tmp_info, count=1)
    sql="select * from information_schema.tables where TABLE_SCHEMA='%s' and TABLE_NAME='%s';" % (mysql_db,bak_global_table)
    sql_result = local_mysql_process.selectAll(sql)
    if sql_result:
        sql = "show create table %s" % bak_global_table
        sql_result = local_mysql_process.selectAll(sql)
        bak_global_table_info = sql_result[0]["Create Table"]
        # 去掉主键AUTO_INCREMENT和AUTO_INCREMENT=40510
        bak_global_table_info = re.sub(r"\bAUTO_INCREMENT\S*?\s\b", "", bak_global_table_info, count=2)
        # 去掉表名
        bak_global_table_info = re.sub(bak_global_table, "", bak_global_table_info, count=1)
        #如果表结构不一致，则需要对原表备份重命名
        if bak_global_table_tmp_info != bak_global_table_info:
            bak_global_table_old = bak_global_table + "_" + str(eval(time.strftime('%Y%m%d%H%M%S')))
            sql = "alter table %s rename to %s" % (bak_global_table, bak_global_table_old)
            local_mysql_process.ddl(sql)
            sql = "alter table %s rename to %s" % (bak_global_table_tmp, bak_global_table)
            local_mysql_process.ddl(sql)
            logger.warning(u"%s表结构存在变更,已将上次备份表重命名为%s" % (global_table,bak_global_table_old))
        else:
            sql = "drop table %s" % (bak_global_table_tmp)
            local_mysql_process.ddl(sql)
    else:
        #如果原来不存在表，则直接创建了
        sql = "alter table %s rename to %s" % (bak_global_table_tmp,bak_global_table)
        local_mysql_process.ddl(sql)

    local_mysql_process.dispose()
    node_process.dispose()

# 更新global_table_status
def update_status(local_mysql_pool,global_table,status,run_mode=0):
    local_mysql_process = dbconn.getProcess(local_mysql_pool)  # mysql线程只需要一个
    fpid = os.getppid()      #父进程pid
    pid = os.getpid()       #本身进程pid
    if status == 0:
        #状态0，表示刚程序刚开始，pid用本身进程pid
        sql = "replace into global_table_status (global_table,pid,run_mode) select '%s',%d,%d" % (global_table,pid,run_mode)
        local_mysql_process.insertOne(sql)
        local_mysql_process.commit()
        local_mysql_process.dispose()
    else:
        #由于可能是并发多进程情况下，所以要按本身进程pid和父进程pid去更新，匹配不到pid就更新不到数据，但是不能漏
        sql = "update global_table_status set status =%d,run_time=TIMESTAMPDIFF(MINUTE ,create_time,update_time) where global_table ='%s' and pid=%d and status=0" % (status,global_table,fpid)
        local_mysql_process.update(sql)
        sql = "update global_table_status set status =%d,run_time=TIMESTAMPDIFF(MINUTE ,create_time,update_time) where global_table ='%s' and pid=%d and status=0" % (status,global_table,pid)
        local_mysql_process.update(sql)
        local_mysql_process.commit()
        local_mysql_process.dispose()