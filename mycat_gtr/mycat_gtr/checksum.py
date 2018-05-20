#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（校验模块）
@Usage:
getid模块获取id流程
模块初始化，按节点分配本地mysql连接和节点连接
1、对所有db进行checksum，结果保存到global_table_chunk
2、同一个chunk不同db之间比较，结果保存到global_table_chunk_differ
3、有差异的chunk精确到行去分析，结果保存到global_table_id_differ
'''


from __future__ import division      #解决per = 100/(count /2000)没有小数的问题
import dbconn
import time
from multiprocessing import Pool
import log_conf
import os
import sys
import execute
import signal

#初始化日志
logger = log_conf.get_logging("checksum")

#checksum运行函数
def run(global_table_g, local_mysql_pool_g, node_pool_list_g, parallel_num_g,run_mode_g,chunk_size_g,primary_key_name_g):
    global global_table
    global local_mysql_process_list
    global node_process_list
    global parallel_num
    global run_mode
    global chunk_size
    global local_mysql_process_ssd
    global primary_key_name

    global_table = global_table_g              #全局表
    local_mysql_process_list = []            #本地mysql线程列表，用于和各个节点对应，每个节点对应一个线程
    node_process_list = []                   #各个节点连接列表
    parallel_num = parallel_num_g         #并发数量
    run_mode = run_mode_g               #执行模式
    chunk_size = chunk_size_g           #一个chunk行数
    primary_key_name = primary_key_name_g #主键名称

    #把本地mysql和节点连接，按节点分配并放入列表
    #因为就只有mycat-gtr使用这些连接池，所以可以长期持有连接，这样后面节省获取连接的代码量，方便直观
    for row in node_pool_list_g:
        local_mysql_process = dbconn.getProcess(local_mysql_pool_g)
        local_mysql_process_list.append({"database": row["database"], "process": local_mysql_process})
        node_process = dbconn.getProcess(row["pool"])
        node_process_list.append({"database": row["database"], "process": node_process})
    local_mysql_process_ssd = dbconn.getProcess(local_mysql_pool_g)  # 专门用于流式游标读取global_table_chunk_differ

    #对所有节点开启事务，RR事务隔离级别，开启事务后，可重复读。借鉴mysqldump，只开启一个事务，事务内用SAVEPOINT保存点，下面每一个chunk的校验后都会返回保存点，读到的数据都会静止，可以降低生产数据变动导致校验不准确的几率。后面不需要对事务回滚，__dispose_process会直接释放
    for row in node_process_list:
        node_process = row["process"]
        node_process.begin()
        node_process.selectAll("SAVEPOINT sp")

    logger.info(u"对全局表id执行checksum检查")
    __sql_checksum_joint()        #拼接校验的sql
    __checksum_init()           #对所有db进行checksum，生成结果保存到global_table_chunk
    __chunk_compare()           #同一个chunk不同db之间比较，结果保存到global_table_chunk_differ
    __chunk_analyze()           #有差异的chunk精确到行去分析，结果保存到global_table_id_differ
    __dispose_process()          #释放连接
    logger.info(u"checksum执行完成")

#拼接checksum校验的sql，借鉴pt-table-checksum的算法
def __sql_checksum_joint():
    global sql_checksum_prepare
    sql="desc %s" % global_table
    node_process = node_process_list[0]["process"]
    sql_result = node_process.selectAll(sql)
    global_table_column_list = []
    global_table_column_isnull_list = []
    for row in sql_result:
        #排除update_time和create_time，可能字段名不标准，所以根据Default和Extra排除
        if row["Default"] != "CURRENT_TIMESTAMP" or row["Extra"] != "on update CURRENT_TIMESTAMP":
            global_table_column_list.append(row["Field"])
            if row["Null"] == "YES":
                global_table_column_isnull_list.append("ISNULL(%s)" % row["Field"])
    global_table_column_str = ",".join(global_table_column_list)
    global_table_column_isnull_str = ",".join(global_table_column_isnull_list)
    sql_head = "SELECT COUNT(*) AS cnt, COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#',"
    sql_middle = ", CONCAT("
    sql_tail = "))) AS UNSIGNED)), 10, 16)), 0) AS crc FROM %s FORCE INDEX(`PRIMARY`) WHERE ((%s >= '%s')) AND ((%s <= '%s')) "
    sql_checksum_prepare = " ".join([sql_head,global_table_column_str,sql_middle,global_table_column_isnull_str,sql_tail])


#检查差异chunk内的每一行记录
def __id_analyze(upper_boundary,sql_checksum):
    checksum_begin_time = time.time()
    logger.debug("checksum_begin_time:%s" % checksum_begin_time)
    #循环检查每个chunk，检查前先初始化变量
    this_crc = None
    this_cnt = None
    last_crc = None
    last_cnt = None
    local_mysql_process = local_mysql_process_list[0]["process"]
    for row in node_process_list:
        #一个id，按每个分片节点db去循环检查
        node_process = row["process"]
        sql_result = node_process.selectAll(sql_checksum)
        node_process.selectAll("ROLLBACK TO SAVEPOINT sp")  # 返回事务保存点
        """
        先从结果做判断，如果某个节点没有结果，则认为不一致
       """
        if sql_result[0]["crc"] != 0 and sql_result[0]["cnt"] != 0 :
            pass        #如果存在结果，则pass跳过
        else:
            #这个id，如果有分片检查结果为空，则说明有节点没有获取值，则记录为有差异
            logger.debug(u"id：%s 存在差异" % upper_boundary)
            #记录差异，差异类型为differ_type=1 某节点丢失数据
            sql = "replace into global_table_id_differ (global_table,global_id,differ_type) select '%s','%s',%d" % (global_table,upper_boundary,1)
            local_mysql_process.update(sql)
            local_mysql_process.commit()
            break
        """
        都有结果了，则从获取的结果判断，上下判断，如果存在不一致就记录
       """
        if last_cnt is not None:
            #如果上一个cnt不为空，则说明这不是该chunk中的第一个分片，需要和当前chunk比较
            this_crc = sql_result[0]["crc"]
            this_cnt = sql_result[0]["cnt"]
            if this_crc != last_crc or this_cnt != last_cnt:
                #如果对比存在差异，更新global_table_id_status
                logger.debug(u"id：%s 存在差异" % upper_boundary)
                #记录差异，差异类型为differ_type=2 字段值不一致
                sql = "replace into global_table_id_differ (global_table,global_id,differ_type) select '%s','%s',%d" % (global_table,upper_boundary,2)
                local_mysql_process.update(sql)
                local_mysql_process.commit()
                break
            else:
                #如果不存在差异，则交换变量
                last_crc = this_crc
                last_cnt = this_cnt
        else:
            #如果上一个cnt为空，则说明是这个chunk中的第一个db分片，需要赋值给last，用于和下一个对比
            last_crc = sql_result[0]["crc"]
            last_cnt = sql_result[0]["cnt"]
    checksum_end_time = time.time()
    logger.debug("checksum_end_time:%s" % checksum_end_time)
    chunk_time = int(checksum_end_time - checksum_begin_time)


#循环检查global_table_chunk_differ每一行
def __chunk_analyze():
    local_mysql_process = local_mysql_process_list[0]["process"]
    sql = "select count(*) as count from global_table_chunk_differ"
    row_sum = local_mysql_process.selectAll(sql)[0]["count"]
    done_num = 0

    sql = "select chunk,upper_boundary,lower_boundary from global_table_chunk_differ"
    local_mysql_process_ssd.execute(sql)
    upper_boundary = None
    while True:
        differ_result = local_mysql_process_ssd.fetchmany(1)    #每次只获取一条
        if differ_result:
            done_num = done_num + 1
            per = done_num / row_sum * 100
            if per < 100:
                logger.info(u"【chunk：%s】存在差异,正在根据id进行检查 %.2f%%" % (differ_result[0]["chunk"], per))
            else:
                logger.info(u"【chunk：%s】存在差异,正在根据id进行检查 100.00%%" % (differ_result[0]["chunk"]))
            #获取下一个chunk的上下界限
            upper_boundary = differ_result[0]["upper_boundary"]
            lower_boundary = differ_result[0]["lower_boundary"]
            while upper_boundary != lower_boundary:
                #如果上界限不等于下界限，说明这个chunk有多条记录，需要循环获取每一行，对每一个id进行分析，这里有个bug，不会对最后一条检查，最后一条交给下面的if执行
                sql_checksum = sql_checksum_prepare % (global_table, primary_key_name,upper_boundary, primary_key_name,upper_boundary)
                #每次传入上界限，也就是一个id，对比这个id的数据
                __id_analyze(upper_boundary,sql_checksum)
                #获取下一条global_id作为上界限
                sql = "select global_id from global_table_id where global_id>'%s' order by global_id limit 1;" % upper_boundary
                upper_boundary = local_mysql_process.selectAll(sql)[0]["global_id"]
            if upper_boundary == lower_boundary:
                #如果上界限等于下界限，说明这可能是chunk的最后一条记录
                #也可能是是这个chunk只有一条，那执行完检查后就退出当前while循环
                sql_checksum = sql_checksum_prepare % (global_table,primary_key_name, upper_boundary, primary_key_name,upper_boundary)
                #每次传入上界限，也就是一个id，对比这个id的数据
                __id_analyze(upper_boundary,sql_checksum)
        else:
            if upper_boundary == None:
                logger.info(u"【恭喜，没有存在chunk差异,退出checksum】")
                # 没有差异，清空global_table_id、global_table_chunk、global_table_chunk_differ，下次不能用模式3，需要模式0或1去重新检查
                local_mysql_process_ssd.dispose()       #先释放流式游标，不然truncate table global_table_chunk_differ会卡住
                sql_truncate = "truncate table global_table_id"
                local_mysql_process.ddl(sql_truncate)
                sql_truncate = "truncate table global_table_chunk"
                local_mysql_process.ddl(sql_truncate)
                sql_truncate = "truncate table global_table_chunk_differ"
                local_mysql_process.ddl(sql_truncate)
                execute.gtr_status = 1  # 更新程序状态
                signal.alarm(1)  # 发送信号14
                time.sleep(1)  # 休眠1秒，等待信号发送完成
                sys.exit(0)
            break
    # 全部按id校验完差异，已经把差异存到global_table_id_differ，清空global_table_id、global_table_chunk、global_table_chunk_differ，下次不能用模式3，需要模式0或1去重新检查
    local_mysql_process_ssd.dispose()       #先释放流式游标，不然truncate table global_table_chunk_differ会卡住
    sql_truncate = "truncate table global_table_id"
    local_mysql_process.ddl(sql_truncate)
    sql_truncate = "truncate table global_table_chunk"
    local_mysql_process.ddl(sql_truncate)
    sql_truncate = "truncate table global_table_chunk_differ"
    local_mysql_process.ddl(sql_truncate)


#chunk对比函数（一个chunk对比所有db节点的结果），把有异常的chunk记录到global_table_chunk_differ
def __chunk_compare():
    logger.info(u"正在对比chunk差异")
    sql_truncate = "truncate table global_table_chunk_differ"
    local_mysql_process=local_mysql_process_list[0]["process"]
    local_mysql_process.ddl(sql_truncate)

    sql = "select max(chunk) as max_chunk from global_table_chunk"
    max_chunk = local_mysql_process.selectAll(sql)[0]["max_chunk"]
    chunk = 1

    while chunk <= max_chunk:
        #循环检查每个chunk，检查前先初始化变量
        last_crc = None
        last_cnt = None
        this_crc = None
        this_cnt = None
        for row in node_process_list:
            #一个chunk中，按每个分片节点db去循环检查
            sql = "select crc,cnt from global_table_chunk where db='%s' and global_table='%s' and chunk=%d" % (row["database"],global_table,chunk)
            sql_result = local_mysql_process.selectAll(sql)
            """
            先从结果做判断，如果某个节点没有结果，则认为不一致
           """
            if sql_result:
                #如果存在结果，则pass跳过
                pass
            else:
                #一个chunk中，如果有分片检查结果为空，则说明有节点没有获取值，则记录为有差异
                sql = "replace into global_table_chunk_differ (global_table,chunk,upper_boundary,lower_boundary) select global_table,chunk,upper_boundary,lower_boundary from global_table_chunk where chunk= %d" % chunk
                local_mysql_process.insertOne(sql)
                local_mysql_process.commit()
                break
            """
            都有结果了，则从获取的结果判断，上下判断，如果存在不一致就记录
           """
            if last_cnt is not None:
                #如果上一个cnt不为空，则说明这不是该chunk中的第一个分片，需要和当前chunk比较
                this_crc = sql_result[0]["crc"]
                this_cnt = sql_result[0]["cnt"]
                if this_crc != last_crc or this_cnt != last_cnt:
                    #如果对比存在差异，则插入global_table_chunk_differ
                    sql = "replace into global_table_chunk_differ (global_table,chunk,upper_boundary,lower_boundary) select global_table,chunk,upper_boundary,lower_boundary from global_table_chunk where chunk= %d" % chunk
                    local_mysql_process.insertOne(sql)
                    local_mysql_process.commit()
                    break
                else:
                    #如果不存在差异，则交换变量
                    last_crc = this_crc
                    last_cnt = this_cnt
            else:
                #如果上一个cnt为空，则说明是这个chunk中的第一个db分片，需要赋值给last，用于和下一个对比
                last_crc = sql_result[0]["crc"]
                last_cnt = sql_result[0]["cnt"]
        chunk = chunk + 1
    logger.info(u"对比chunk差异完成")


#checksum并发执行函数，把结果记录到global_table_checksums
def __checksum_parallel(chunk, upper_boundary, lower_boundary, sql_checksum, db_name):
    logger.debug("chunk:%s" % chunk)
    logger.debug("upper_boundary:%s" % upper_boundary)
    logger.debug("lower_boundary:%s" % lower_boundary)
    logger.debug("sql_checksum:%s" % sql_checksum)
    logger.debug("db_name:%s" % db_name)
    logger.debug("process(%s) __checksum_parallel.db_name:%s" % (os.getpid(),db_name))
    checksum_begin_time = time.time()
    logger.debug("process(%s) checksum_begin_time:%s" % (os.getpid(),checksum_begin_time))
    for row in node_process_list:
        if row["database"] == db_name:
            logger.debug(u"process(%s) node_process_list匹配OK，db是：%s" % (os.getpid(),db_name))
            node_process = row["process"]
    #结果返回cnt行数，crc计算的checksum值
    logger.debug(u"process(%s) 开始获取cnt和crc" % os.getpid())
    sql_result = node_process.selectAll(sql_checksum)
    node_process.selectAll("ROLLBACK TO SAVEPOINT sp")    #返回事务保存点
    cnt = sql_result[0]["cnt"]
    crc = sql_result[0]["crc"]
    logger.debug(u"process(%s) cnt是：%s  ；crc是：%s" % (os.getpid(),cnt,crc))
    logger.debug(u"process(%s) 结束获取，断开连接" % os.getpid())
    checksum_end_time = time.time()
    logger.debug("process(%s) checksum_end_time:%s" % (os.getpid(),checksum_end_time))
    chunk_time = checksum_end_time - checksum_begin_time
    logger.debug(u"process(%s) 耗时：%d" % (os.getpid(),chunk_time))
    logger.debug(u"process(%s) 把cnt和crc插入到global_table_checksums" % os.getpid())
    sql = "replace into global_table_chunk \
          (db,global_table,chunk,chunk_time,upper_boundary,lower_boundary,crc,cnt) \
          values ('%s','%s',%s,%s,'%s','%s','%s',%s)" % (db_name, global_table, chunk, chunk_time, upper_boundary, lower_boundary, crc, cnt)
    for row in local_mysql_process_list:
        if row["database"] == db_name:
            logger.debug(u"process(%s) node_mysql_process_list匹配OK，db是：%s" % (os.getpid(), db_name))
            local_mysql_process = row["process"]
    local_mysql_process.insertOne(sql)
    #只提交，不释放，因为下一个chunk还需要
    local_mysql_process.commit()
    logger.debug(u"process(%s) 插入成功，断开连接" % os.getpid())
    #执行结束后，返回当前操作节点名称
    return db_name


#checksum入口，获取id上界限和下界限
def __checksum_init():
    local_mysql_process = local_mysql_process_list[0]["process"]
    sql_min_global_id = "select global_id from global_table_id order by global_id limit 1"
    sql_max_global_id = "select global_id from global_table_id order by global_id desc limit 1"
    sql_results = local_mysql_process.selectAll(sql_min_global_id)
    if not sql_results:
        #如果不存在数据，则退出，主要是防止还没有拉取数据获取global_id就直接用模式3
        logger.warning(u"没有找到global_id，请使用模式0或模式1获取global_id")
        execute.gtr_status = 5  # 更新程序状态
        signal.alarm(1)  # 发送信号14
        time.sleep(1)  # 休眠1秒，等待信号发送完成
        sys.exit(0)
    min_global_id = sql_results[0]["global_id"] #最小值
    max_global_id = local_mysql_process.selectAll(sql_max_global_id)[0]["global_id"] #最大值

    p = Pool(parallel_num)  #创建进程池，用于并发校验
    upper_boundary = ""    #上界限
    lower_boundary = ""    #下界限
    tmp_boundary = ""    #中间值交换
    p_results = []     #接收子进程返回结果

    #根据运行模式初始化第一个chunk位置
    if run_mode == 3:
        #断点续检查
        logger.info(u"断点续检查，从上一个chunk开始检查")
        sql = "select ifnull(max(chunk),0) as chunk from global_table_chunk;"
        sql_results = local_mysql_process.selectAll(sql)[0]["chunk"]
        if sql_results != 0:
            chunk = int(sql_results) - 1     #当前chunk可能所有db都没有返回结束，所以用上一个chunk
            logger.info(u"上一个chunk：%d" % chunk)
            sql = "select lower_boundary from global_table_chunk where chunk=%d limit 1;" % chunk       #根据chunk获取下界限
            tmp_boundary = local_mysql_process.selectAll(sql)[0]["lower_boundary"]  #把上一个chunk的下界限交给中间值，该下界限会作为下一个chunk的上界限，其实相当于重复检查这个id
            chunk = chunk + 1  #把chunk加回1，因为上一个chunk的下界限交给中间值，要获取的是下一个chunk。不加1该chunk会被覆盖，如果该chunk有差异则会检查不出来
        else:
            sql_truncate = "truncate table global_table_chunk"
            local_mysql_process.ddl(sql_truncate)
            chunk = 1  # 初始化chunk=1
            tmp_boundary = min_global_id  # 把最小的id交给中间值
    else:
        #非断点续检查，全部重新检查
        sql_truncate = "truncate table global_table_chunk"
        local_mysql_process.ddl(sql_truncate)
        chunk = 1          #初始化chunk=1
        tmp_boundary = min_global_id        #把最小的id交给中间值

    #开始checksum校验
    logger.info(u"正在获取总行数计算百分比")
    sql = "select count(*) as count from global_table_id"
    row_sum = local_mysql_process.selectAll(sql)[0]["count"]    #获取总行数，用于计算完成的百分比
    begin_time = time.time()
    while lower_boundary != max_global_id:
        end_time = time.time()
        # 下面输出完成百分比，每5秒钟输出一次
        if end_time - begin_time > 5:
            per = (chunk*chunk_size)/row_sum * 100
            if per < 100:
                logger.info(u"正在获取校验结果【chunk %d】 %.2f%%" % (chunk,per))  # 保留两位小数
            else:
                logger.info(u"正在获取校验结果【chunk %d】 100.00%%" % (chunk))
            begin_time = end_time  # 把结束时间赋值给开始时间
        #获取上下界限
        sql_upper_lower_boundary = "select global_id from global_table_id where global_id>='%s' order by global_id limit %s,2" % (tmp_boundary, chunk_size)
        upper_lower_boundary = local_mysql_process.selectAll(sql_upper_lower_boundary)
        if upper_lower_boundary:
            #结果不为空，则交换变量
            logger.debug(u"上下线结果输出：%s" % upper_lower_boundary)
            logger.debug(u"上线结果：%s" % upper_lower_boundary[0]["global_id"])
            logger.debug(u"下线结果：%s" % upper_lower_boundary[1]["global_id"])
            upper_boundary = tmp_boundary           #中间值交给上界限
            lower_boundary = upper_lower_boundary[0]["global_id"]        #limit 第一个值交给下界限
            tmp_boundary = upper_lower_boundary[1]["global_id"]          #limit 第二个值交给中间值，用于计算下一个chunk
        else:
            #如果结果为空，说明剩余行数不够一个chunk_size，则用max_global_id作为下界限
            #这个时候tmp_boundary有可能等于max_global_id，或者tmp_boundary小于max_global_id
            upper_boundary = tmp_boundary       #中间值交给上界限
            lower_boundary = max_global_id      #最大值交给下界限
            tmp_boundary = max_global_id        #最大值交给中间值

        #拼装sql，分发到所有db节点执行
        sql_checksum = sql_checksum_prepare % (global_table, primary_key_name,upper_boundary, primary_key_name,lower_boundary)
        logger.debug("parallel process insert")
        for node_process in node_process_list:
            db_name = node_process["database"]
            logger.debug("__checksum_parallel_init.node_pool_db:%s" % db_name)
            logger.debug("%s into process pool" % db_name)
            p_results.append(p.apply_async(__checksum_parallel,args=(chunk, upper_boundary, lower_boundary, sql_checksum, db_name,)))
        for row in p_results:
            row.wait()  #等待进程函数执行完毕
        for row in p_results:
            if row.ready():  #进程函数是否已经启动了
                if row.successful():  #进程函数是否执行成功
                    pass
                else:
                    logger.error(u"【chunk】:%d 节点%s子进程没有执行成功" % (chunk,row.get()))
            else:
                logger.error(u"【chunk】:%d 节点%s子进程没有启动成功" % (chunk, row.get()))
        chunk = chunk + 1
    p.close()       #关闭进程池，不允许添加子进程任务
    logger.debug("Waiting for all subprocesses done")
    p.join()        #等待所有子进程结束
    logger.debug("all subprocesses done,close subprocesses")


#释放连接
def __dispose_process():
    for row in local_mysql_process_list:
        row["process"].dispose()
    for row in node_process_list:
        row["process"].dispose()
    local_mysql_process_ssd.dispose()
