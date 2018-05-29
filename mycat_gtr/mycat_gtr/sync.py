#coding=utf-8
'''
@Author: tanzh
@Date: 20171215
@Description: mycat全局表不一致修复（修复模块）
@Usage:
sync修复函数
模块初始化先拿到所有节点的mysql线程连接，下面修复步骤需要在这些连接中进行
1、判断差异数量是否超过auto_repair_size，超过则不修复
2、循环对所有节点开启事务start transaction，并加S锁lock in share mode，检查是否有节点报错
3、只存在一个唯一索引，每个id判断是否对应多个唯一索引值，对应多个则返回
4、存在一个唯一索引，一个id对应一个索引值，或者没有唯一索引，则遍历所有节点，根据time_column找出时间最大的记录，如果存在物理删除则返回
5、修复前先备份各个节点的数据
6、从数据最新的节点中获取数据拼接成replace into 修复语句
7、循环对所有节点执行replace into 语句，检查是否有节点报错
8、循环对所有节点进行commit提交，检查是否有节点报错
9、更新global_table_id_differ的status=1

注意：
当前模块会存在一种情况，因为global_table_id_differ表的唯一索引是(global_table,global_id,status,create_time)，允许不断追加，只要存在差异，一个global_id会有多条记录
'''


from __future__ import division      #解决per = 100/(count /2000)没有小数的问题
import log_conf
import dbconn_xa
import time
import sys
import execute
import signal

logger = log_conf.get_logging("sync")

#设置模块全局变量
def run(global_table_g,local_mysql_pool_g,node_pool_list_g,auto_repair_size_g,bak_global_table_g,primary_key_name_g,time_column_g,time_value_g,unique_column_list_g):
    global global_table
    global local_mysql_process
    global local_mysql_process_ssd
    global node_process_list
    global auto_repair_size
    global bak_global_table
    global primary_key_name
    global time_column
    global time_value
    global unique_column_list

    global_table = global_table_g    #全局表
    auto_repair_size = auto_repair_size_g     #自动修复数量
    bak_global_table = bak_global_table_g     #备份表名
    node_process_list = []            #各个节点连接列表
    primary_key_name = primary_key_name_g #主键名称
    time_column = time_column_g     #时间字段
    time_value = time_value_g   #获取时间内容
    unique_column_list = unique_column_list_g    #唯一索引字段列表

    #把本地mysql和节点连接，按节点分配并放入列表
    #因为就只有mycat-gtr使用这些连接池，所以可以长期持有连接，这样后面节省获取连接的代码量，方便直观
    for row in node_pool_list_g:
        node_process = dbconn_xa.getProcess(row["pool"])
        node_process_list.append({"database": row["database"],"datanode": row["datanode"], "process": node_process})
    local_mysql_process = dbconn_xa.getProcess(local_mysql_pool_g)     #mysql线程用于记录数据
    local_mysql_process_ssd = dbconn_xa.getProcess(local_mysql_pool_g)   #专门用于流式游标读取global_table_id_differ

    logger.info(u"开始执行修复")
    __sync_init()          #sync入口
    __dispose_process()    #释放连接
    logger.info(u"sync执行完成")


def __sync_execute(id_differ,global_id_differ):
    #循环对所有节点开启事务start transaction，并加S锁lock in share mode，检查是否有节点报错
    #循环对所有节点开启事务start transaction
    logger.info(u"%s='%s'<对所有节点开启事务>" % (primary_key_name,global_id_differ))
    for row in node_process_list:
        node_process = row["process"]
        node_process.begin()
    logger.info(u"%s='%s'对所有节点开启事务成功" % (primary_key_name,global_id_differ))
    #循环对所有节点进行加S锁lock in share mode，检查是否有节点报错
    sql_share_lock = "select * from %s where %s = '%s' lock in share mode" % (global_table,primary_key_name,global_id_differ)
    logger.info(u"%s='%s'<对所有节点加S锁>" % (primary_key_name,global_id_differ))
    for row in node_process_list:
        node_process = row["process"]
        logger.info(u"%s='%s'正在对节点%s加锁" % (primary_key_name,global_id_differ, row["datanode"]))
        #执行上锁，过程中如果遇到错误，则更新is_err
        is_err = node_process.update(sql_share_lock)
        if is_err == -1 or is_err == 'False':
            logger.error(u"%s='%s'对节点%s加锁失败" % (primary_key_name,global_id_differ,row["datanode"]))
            logger.info(u"%s='%s'对所有节点回滚事务" % (primary_key_name,global_id_differ))
            for row in node_process_list:
                node_process = row["process"]
                node_process.rollback()
            # 更新记录状态为3，标记为加锁失败
            sql = "update global_table_id_differ set status=3 where global_table = '%s' and global_id = '%s' and id='%s'" % (global_table, global_id_differ,id_differ)
            local_mysql_process.update(sql)
            local_mysql_process.commit()
            # 返回0，结束当前记录的修复
            return 0
    logger.info(u"%s='%s'对所有节点加S锁成功" % (primary_key_name,global_id_differ))

    #只存在一个唯一索引，判断是否一个id对应多个唯一索引值，一个对应多个唯一索引值则返回
    if unique_column_list != []:
        #唯一索引字段列表不为空，需要判断索引值是否只有一个可以继续修复，有多个则返回
        #unique_column_list_dict格式例子[{key:value,key2:value2},{key3:value3,key4:value4}]
        unique_key_value_list=[]
        unique_key_value_list_tmp = []
        sql = "select %s from %s where %s='%s'" % (",".join(unique_column_list),global_table,primary_key_name,global_id_differ)
        for row in node_process_list:
            node_process = row["process"]
            sql_result = node_process.selectAll(sql)
            if  sql_result:
                #要存在结果，全部都追加到列表
                unique_key_value_list_tmp.append(sql_result[0])
        #对各个节点唯一索引去重
        for row in unique_key_value_list_tmp:
            if row not in unique_key_value_list:
                unique_key_value_list.append(row)
        if len(unique_key_value_list) > 1:
            logger.info(u"%s='%s'对应了多个唯一索引值，不执行修复" % (primary_key_name,global_id_differ))
            logger.info(u"%s='%s'对所有节点回滚事务" % (primary_key_name,global_id_differ))
            for row in node_process_list:
                node_process = row["process"]
                node_process.rollback()
            #一个id有多个唯一索引值，暂时返回不修复，状态为4，标记修复失败，差异类型更新为3
            sql = "update global_table_id_differ set status=4,differ_type=3 where global_table = '%s' and global_id = '%s' and id='%s'" % (global_table, global_id_differ,id_differ)
            local_mysql_process.update(sql)
            local_mysql_process.commit()
            #返回0，结束当前记录的修复
            return 0

    #存在一个唯一索引，一个id对应一个索引值，或者没有唯一索引，则执行下面修复操作
    #遍历所有节点，根据time_column找出时间最大的记录
    #latest_line记录最新的一条记录是那一个节点的，格式{"database":db1,"datanode":nd1,"process":process,"timestamp":"时间戳"}
    latest_line = {}
    time_value_list = []
    time_value_list_tmp = []
    sql = "select * from %s where %s = '%s' " % (global_table,primary_key_name,global_id_differ)
    for row in node_process_list:
        #循环所有节点获取数据
        node_process = row["process"]
        sql_result = node_process.selectAll(sql)
        if sql_result:
            #必须要存在结果，没有结果直接查找下一个节点，遍历所以都没有结果，则说明这条记录已经被物理删除
            time_value_tmp = eval(time_value % (sql_result[0][time_column]))
            time_value_list_tmp.append(int(time_value_tmp))
            if latest_line == {}:
                #如果latest_line是空，则是第一条记录，直接赋值
                latest_line["database"] = row["database"]
                latest_line["datanode"] = row["datanode"]
                latest_line["process"] = row["process"]
                latest_line["timestamp"] = int(time_value_tmp)
            elif int(time_value_tmp) > int(latest_line["timestamp"]):
                #如果latest_line不为空，则不是第一条记录，大于之前的时间戳，则记录当前节点信息
                latest_line["database"] = row["database"]
                latest_line["datanode"] = row["datanode"]
                latest_line["process"] = row["process"]
                latest_line["timestamp"] = int(time_value_tmp)
    # 对各个节点time_value去重，如果所有节点字段时间值都一样，则不自动修复
    for row in time_value_list_tmp:
        if row not in time_value_list:
            time_value_list.append(row)
    #如果time_value_list_tmp列表大于1个值，说明存在多个节点有数据，需要检查time_value是否一致
    if len(time_value_list_tmp) > 1 and len(time_value_list) == 1:
        logger.info(u"%s='%s'所有节点%s字段值一致，不能自动修复" % (primary_key_name,global_id_differ,time_column))
        logger.info(u"%s='%s'对所有节点回滚事务" % (primary_key_name,global_id_differ))
        for row in node_process_list:
            node_process = row["process"]
            node_process.rollback()
        #更新记录状态为5，标记为已经物理删除
        sql = "update global_table_id_differ set status=5 where global_table = '%s' and global_id = '%s' and id='%s'" % (global_table, global_id_differ,id_differ)
        local_mysql_process.update(sql)
        local_mysql_process.commit()
        return 0
    if latest_line == {}:
        logger.info(u"%s='%s'已经被物理删除，不需要修复" % (primary_key_name,global_id_differ))
        logger.info(u"%s='%s'对所有节点回滚事务" % (primary_key_name,global_id_differ))
        for row in node_process_list:
            node_process = row["process"]
            node_process.rollback()
        #更新记录状态为2，标记为已经物理删除
        sql = "update global_table_id_differ set status=2 where global_table = '%s' and global_id = '%s' and id='%s'" % (global_table, global_id_differ,id_differ)
        local_mysql_process.update(sql)
        local_mysql_process.commit()
        #返回0，结束当前记录的修复
        return 0
    else:
        logger.info(u"%s='%s'以节点%s数据为准，对所有节点进行修复" % (primary_key_name,global_id_differ, latest_line["datanode"]))

    #修复前先备份各个节点的数据
    logger.info(u"%s='%s'正在备份数据" % (primary_key_name,global_id_differ))
    for row in node_process_list:
        # 记录字段名和字段值，用来拼接replace into 语句
        column_name = []
        column_value = []
        occupy = []  # 入参占位符，一个字段一个%s
        sql = "select * from %s where %s = '%s' " % (global_table, primary_key_name,global_id_differ)
        node_process = row["process"]
        sql_result = node_process.selectAll(sql)
        if sql_result:
            for key, value in sql_result[0].iteritems():
                column_name.append(key)
                column_value.append(value)
                occupy.append("%s")
            column_name.append("gtr_source_dn")
            column_value.append(row["datanode"])
            occupy.append("%s")
            column_name.append("gtr_differ_id")
            column_value.append(id_differ)
            occupy.append("%s")
            if row["datanode"] == latest_line["datanode"]:
                column_name.append("gtr_repair_standard")
                column_value.append(1)
                occupy.append("%s")
            sql_bak = "replace into %s (%s) select %s" % (bak_global_table, ",".join(column_name), ",".join(occupy))
            local_mysql_process.update(sql_bak, column_value)
            local_mysql_process.commit()

    #从数据最新的节点中获取数据拼接成replace into 修复语句
    #记录字段名和字段值，用来拼接replace into 语句
    logger.info(u"%s='%s'正在生成修复语句" % (primary_key_name,global_id_differ))
    column_name = []
    column_value = []
    occupy = []   #入参占位符，一个字段一个%s
    sql = "select * from %s where %s = '%s' " % (global_table,primary_key_name,global_id_differ)
    latest_node_process = latest_line["process"]
    sql_result = latest_node_process.selectAll(sql)
    for key,value in sql_result[0].iteritems():
        logger.debug("key: %s" % key)
        logger.debug("value: %s" % value)
        logger.debug("value_type:%s" % type(value))
        column_name.append(key)
        column_value.append(value)
        occupy.append("%s")
    logger.debug("%s" % column_name)
    logger.debug("%s" % column_value)
    #replace into 修复语句
    sql_repair = "replace into %s (%s) select %s" % (global_table,",".join(column_name),",".join(occupy))

    #循环对所有节点执行replace into 语句，检查是否有节点报错
    logger.info(u"%s='%s'<对所有节点修复>" % (primary_key_name,global_id_differ))
    for row in node_process_list:
        node_process = row["process"]
        logger.info(u"%s='%s'正在对节点%s修复" % (primary_key_name,global_id_differ, row["datanode"]))
        #执行replace into，过程中如果遇到错误，则更新is_err
        #修复bug，参数通过column_value另外传入到dbconn_xa的param，这样可以对特殊字符自动转义，先拼接成一个sql则无法对特殊字符转义
        is_err = node_process.update(sql_repair,column_value)
        if is_err == -1 or is_err == 'False':
            logger.error(u"%s='%s'修复失败" % (primary_key_name,global_id_differ))
            logger.error(u"对所有节点回滚事务")
            for row in node_process_list:
                node_process = row["process"]
                node_process.rollback()
            # 更新记录状态为4，标记为修复失败
            sql = "update global_table_id_differ set status=4 where global_table = '%s' and global_id = '%s' and id='%s'" % (global_table, global_id_differ,id_differ)
            local_mysql_process.update(sql)
            local_mysql_process.commit()
            # 返回0，结束当前记录的修复
            return 0
    logger.info(u"%s='%s'对所有节点修复成功" % (primary_key_name,global_id_differ))

    #循环对所有节点进行commit提交，检查是否有节点报错
    logger.info(u"%s='%s'对所有节点执行提交" % (primary_key_name,global_id_differ))
    for row in node_process_list:
        node_process = row["process"]
        node_process.commit()

    #更新global_table_id_differ的status=1，已修复
    sql = "update global_table_id_differ set status=1 where global_table = '%s' and global_id = '%s' and id='%s' " % (global_table,global_id_differ,id_differ)
    local_mysql_process.update(sql)
    local_mysql_process.commit()


#sync分发函数，用于分发id
def __sync_init():
    # 判断差异数量是否超过auto_repair_size，超过则不修复
    # 这里不用流式游标了，把计算交给mysql是最好的，上百万差异就等几分钟。如果是用流式游标循环获取要循环上百万次；或者用流式游标一次获取上百万，python内存也会暴增
    sql = "select count(*) as differ_count from global_table_id_differ where status = 0 and global_table = '%s' " % (global_table)
    differ_count = local_mysql_process.selectAll(sql)[0]["differ_count"]
    if auto_repair_size != 0:
        if differ_count > auto_repair_size:
            logger.warning(u"差异数量%d超过auto_repair_size=%d，不执行修复" % (differ_count,auto_repair_size))
            execute.gtr_status = 7  # 更新程序状态
            signal.alarm(1)  # 发送信号14
            time.sleep(1)  # 休眠1秒，等待信号发送完成
            sys.exit(0)

    # 获取差异id，因为如果差异数量过多，全部读进内存会oom，所以用流式游标
    #注意：这里流式游标一定要独立一个线程local_mysql_process_ssd，因为__sync_execute也使用到local_mysql_process，如果流式游标不独立一个，那就无法读取下一条global_table_id_differ
    sql = "select id,global_id from global_table_id_differ where status = 0 and global_table = '%s' " % (global_table)
    local_mysql_process_ssd.execute(sql)
    id_differ = None
    done_num = 0
    while True:
        differ_result = local_mysql_process_ssd.fetchmany(1)    #每次只获取一条
        if differ_result:
            id_differ = differ_result[0]["id"]
            global_id_differ = differ_result[0]["global_id"]
            done_num = done_num + 1
            per = done_num / differ_count * 100
            if per < 100:
                logger.info(u"%s='%s'正在进行修复------------------------------------ %.2f%%" % (primary_key_name,global_id_differ,per))
            else:
                logger.info(u"%s='%s'正在进行修复------------------------------------ 100.00%%" % (primary_key_name,global_id_differ))
            __sync_execute(id_differ,global_id_differ)
        else:
            if id_differ == None:
                logger.info(u"【恭喜，没有存在差异，退出sync】")
                return 0
            break
    logger.info(u"差异记录查看global_table_id_differ，备份查看%s" % bak_global_table)


#释放连接
def __dispose_process():
    local_mysql_process.dispose()
    local_mysql_process_ssd.dispose()
    for row in node_process_list:
        row["process"].dispose()