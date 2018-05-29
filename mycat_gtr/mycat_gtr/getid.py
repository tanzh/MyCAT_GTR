#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（获取全局表id模块）
@Usage:
getid模块获取id流程
模块初始化，按节点分配本地mysql连接和节点连接
1、检查各个节点表结构是否一致
2、创建临时表
3、用流式游标并发把id导入本地mysql
4、把id去重插入global_table_id
5、清理临时表，清理本地mysql连接，清理节点连接
'''


from __future__ import division      #解决per = 100/(count /2000)没有小数的问题
from multiprocessing import Pool,Process
import dbconn
import log_conf
import os
import time
import sys
import execute
import signal
import re

#初始化日志
logger = log_conf.get_logging("getid")

#getid运行函数
def run(global_table_g, local_mysql_pool_g, node_pool_list_g,parallel_num_g,getid_size_g,primary_key_name_g,primary_key_type_g):
    global global_table
    global local_mysql_process_list
    global node_process_list
    global node_table_list
    global parallel_num
    global getid_size
    global primary_key_name
    global primary_key_type
    global local_mysql_process_per

    global_table = global_table_g    #全局表
    local_mysql_process_list = []     #本地mysql线程列表，用于和各个节点对应，每个节点对应一个线程
    node_process_list = []        #各个节点连接列表
    node_table_list = []    #存储本地mysql对应各个节点的名字
    parallel_num = parallel_num_g     #并发数量
    getid_size = getid_size_g        #拉取数据，每次拉取数量
    primary_key_name = primary_key_name_g  #主键名称
    primary_key_type = primary_key_type_g  #主鍵類型

    #把本地mysql和节点连接，按节点分配并放入列表
    #因为就只有mycat-gtr使用这些连接池，所以可以长期持有连接，这样后面节省获取连接的代码量，方便直观
    for row in node_pool_list_g:
        local_mysql_process = dbconn.getProcess(local_mysql_pool_g)
        local_mysql_process_list.append({"database": row["database"],"datanode": row["datanode"], "process": local_mysql_process})
        node_process = dbconn.getProcess(row["pool"])
        node_process_list.append({"database": row["database"],"datanode": row["datanode"], "process": node_process})
    local_mysql_process_per = dbconn.getProcess(local_mysql_pool_g)  # 专门用于计算百分比

    logger.info(u"开始获取相关节点全局表id")
    __create_tmp_table()     #创建临时表
    __id_into_tmp_table()   #把id插入临时表
    __union_insert()        #汇总去重插入global_table_id
    __drop_tmp_table()      #清理临时表
    logger.info(u"获取全局表id完成")


#每个节点对应创建一个实体表，并把表名和分片对应关系添加到node_table_list
#node_table_list格式：{"datanode":"nd1", "table_name": "table_name_db1"}
def __create_tmp_table():
    logger.info(u"正在本地mysql创建临时表")
    #临时表模板
    sql_innodb='create table global_table_template( \
        id int primary key AUTO_INCREMENT comment "主键", \
        global_id %s comment "业务表id" \
          ) comment "每个分片节点具体的id"' % primary_key_type
    local_mysql_process=local_mysql_process_list[0]["process"]
    for row in node_process_list:
        new_table_name = global_table + "_" + row["datanode"]      #拼接表名
        sql = sql_innodb.replace("global_table_template",new_table_name)  #替换表名
        sql_drop="drop table if exists %s" % new_table_name
        local_mysql_process.ddl(sql_drop)        #先删除
        local_mysql_process.ddl(sql)             #后创建
        node_table_list.append({"datanode": row["datanode"], "table_name": new_table_name})    #添加到列表


# id全部插入到global_table_id以后，则删除临时表，释放连接
def __drop_tmp_table():
    logger.info(u"正在清理临时表")
    local_mysql_process = local_mysql_process_list[0]["process"]
    for row in node_table_list:
        sql = "drop table %s" % row["table_name"]
        local_mysql_process.ddl(sql)
    for row in local_mysql_process_list:
        row["process"].dispose()
    for row in node_process_list:
        row["process"].dispose()
    local_mysql_process_per.dispose()


#子进程，把数据从节点流式游标插入到本地mysql实体表
def __insert_process(node_table):
    logger.debug(u"process(%s) 开启一个连接" % os.getpid())
    for row in node_process_list:
        if node_table["datanode"] == row["datanode"]:
            node_process = row["process"]
    for row in local_mysql_process_list:
        if node_table["datanode"] == row["datanode"]:
            local_mysql_process = row["process"]
    logger.debug(u"process(%s) 准备插入" % os.getpid())
    sql = "select %s from %s" % (primary_key_name,global_table)
    node_process.execute(sql)
    insert_num = 1
    while True:
        id = node_process.fetchmany(getid_size)    #每次获取getid_size条
        if id:
            id_list = []
            for row in id:
                #因为有可能是整型，所以要str转换
                id_list.append(str(row[primary_key_name]))
            sql = "insert into %s (global_id)  values ('%s') " % (node_table["table_name"], "'),('".join(id_list))
            local_mysql_process.insertOne(sql)
            insert_num = insert_num + 1
            #五千条提交一次，减轻local mysql压力
            if getid_size*insert_num > 5000:
                local_mysql_process.commit()
                insert_num = 1
        else:
            break
    local_mysql_process.commit()    #结束之后要提交一次，防止最好的批次不够数量导致连接一直没有提交
    logger.info("process(%s) %s done" % node_table["table_name"])


#获取拉取进度
def per_check(row_sum):
    begin_time = time.time()
    if row_sum != 0:
        while True:
            end_time = time.time()
            if end_time - begin_time > 5:
                per_sum = 0
                for row in node_table_list:
                    sql = "show table status like '%s'" % row["table_name"]
                    node_row = local_mysql_process_per.selectAll(sql)[0]["Rows"]  # 从统计信息大概获取总行数，用于计算完成的百分比
                    per_sum = per_sum + node_row
                per = per_sum / row_sum * 100
                if per < 100:
                    logger.info("正在拉取全局表id插入临时表 %.2f%%" % per)  # 保留两位小数
                else:
                    logger.info("正在拉取全局表id插入临时表 100.00%")
                    break       #达到100%则跳出循环
                begin_time = end_time  # 把结束时间赋值给开始时间


#父进程，把数据从节点流式游标插入到本地mysql实体表
#如果数据量大节点多，则会维持很久，但是由于用流式游标，CPU/内存方面会保持稳定，属于大操作，类似大事务
def __id_into_tmp_table():
    logger.info(u"正在拉取全局表id插入临时表")
    sql_truncate="truncate table global_table_id;"
    local_mysql_process=local_mysql_process_list[0]["process"]
    local_mysql_process.ddl(sql_truncate)    #先清空global_table_id
    logger.debug("parallel process insert")
    #从所有节点中找到要拉取的总行数
    row_sum = 0
    for row in node_process_list:
        sql = "show table status like '%s'" % global_table
        node_process = row["process"]
        node_row = node_process.selectAll(sql)[0]["Rows"]    #从统计信息大概获取总行数，用于计算完成的百分比
        row_sum = row_sum +node_row
    p = Pool(parallel_num)
    for row in node_table_list:
        logger.debug("%s into process pool" % row)
        p.apply_async(__insert_process, args=(row,))
    get = Process(target=per_check, args=(row_sum,))       #另外开启一个进程，获取拉取进度（不放入进程池，如果进程池满了，就会导致无法同时执行）
    get.start()
    p.close()
    p.join()
    get.join()         #上面所有拉取数据子进程都结束了，等待获取拉取进度的进程停止


#所有节点插入结束后，通过union合并插入到global_table_id
#每次只取小范围进行union，拆成小事务，防止发生oom内存溢出
def __union_insert():
    logger.info(u"正在对全局表id汇总去重")
    local_mysql_process = local_mysql_process_list[0]["process"]
    max_id = 0
    tmp_id = 0
    min_id = 0
    #遍历临时表，找出最大id，这里一定要求最大id，不能用count，因为去重拆分成小范围是按id范围
    for row in node_table_list:
        sql = "select max(id) as id from %s" % row["table_name"]
        tmp_id = local_mysql_process.selectAll(sql)[0]["id"]
        if tmp_id >= max_id:
            max_id = tmp_id
    union_size = 2000          #union去重语句，每次只取2000条
    done_size = 0            #union去重完成的数量
    begin_time = time.time()
    sql_head = "replace into global_table_id (global_id) select t.global_id from ("    #需要拼接sql的头部
    while min_id <= max_id:
        tmp_id = min_id + union_size
        sql_middle = None
        for row in node_table_list:
            sql = "select global_id from %s where %d<=id and id <%d" % (row["table_name"],min_id,tmp_id)
            if node_table_list.index(row) == 0:
                sql_middle = " ".join([sql,"union"])    #第一条，后面拼接union，并交给sql_middle
            if (node_table_list.index(row) + 1) != len(node_table_list):
                sql_middle = " ".join([sql_middle, sql, "union"])   #非第一条和最后一条，拼接sql_middle，sql，"union"，并交给sql_middle
            else:
                sql_middle = " ".join([sql_middle, sql])    #最后一条，拼接sql_middle，sql，后面不拼接union
        sql = " ".join([sql_head,sql_middle,") t"])
        local_mysql_process.insertOne(sql)
        local_mysql_process.commit()
        min_id = tmp_id
        #下面输出完成百分比，每5秒钟输出一次
        done_size = done_size + union_size
        end_time = time.time()
        if end_time - begin_time > 5:
            per = done_size / max_id * 100
            if per < 100:
                logger.info("正在对全局表id汇总去重 %.2f%%" % per)  # 保留两位小数
            else:
                logger.info("正在对全局表id汇总去重 100.00%")
            begin_time = end_time  # 把结束时间赋值给开始时间
    #汇总去重结束后要判断是否有数据
    sql = "select global_id from global_table_id limit 1"
    sql_results = local_mysql_process.selectAll(sql)
    if not sql_results:
        logger.warning(u"没有找到global_id，全局表%s可能不存在数据" % global_table)
        __drop_tmp_table()     #删除临时表
        execute.gtr_status = 4  # 更新程序状态
        signal.alarm(1)  # 发送信号14
        time.sleep(1)  # 休眠1秒，等待信号发送完成
        sys.exit(0)

