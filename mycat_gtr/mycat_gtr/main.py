#coding=utf-8
'''
@Author: tanzh(qq:405924341)
@Inspired by eric.cheng(qq:4897393)
@Date: 20171215
@Description: mycat全局表不一致修复（主程序）
@Usage:
'''

import ConfigParser
import log_conf
import dbpool
import dbconn
import getid
import checksum
import sync
import warnings
import time
import sys
import pymysql
import signal
import os
import commands
import init
import execute
try:
  import xml.etree.cElementTree as ET
except ImportError:
  import xml.etree.ElementTree as ET

#忽略警告，比如Warning: Unknown table
warnings.filterwarnings('ignore')

#暴力退出，ctrl+c和被kill，记录程序状态（由于多进程无法ctrl+c退出，所以通过这种方式）
def __abort_quit(signum, frame):
    # 更新程序运行状态
    init.update_status(local_mysql_pool, global_table, 2)
    #kill掉程序
    fpid = os.getppid()     #父进程pid
    pid = os.getpid()       #本身进程pid
    fpid_name = commands.getstatusoutput("ps -ef| awk '{if($2==%d){print $NF}}'" % (fpid))[1]
    #针对并发多进程，比如数据拉取的时候
    if "bash" not in fpid_name:
        # 杀死父进程，排除bash进程
        commands.getstatusoutput("kill -9 %d" % fpid)
        #失去父进程的子进程会置顶父进程为1，需要kill掉
        commands.getstatusoutput("ps -ef | grep gtr | grep -v grep | awk '{if($3==1){print $0}}' |awk '{print $2}' | xargs -i kill -9 {}")
    #针对单进程，比如汇总去重的时候
    commands.getstatusoutput("kill -9 %d" % pid)

#普通退出，logger抛出警告和错误主动退出，记录程序状态（这里为什么要和暴力退出区分，为什么不用kill，因为如果普通退出被kill，屏幕会显示被kill，并且$?状态不是0，批量修复mycat_gtr.sh脚本不好判断）
def __normal_quit(signum, frame):
    # 更新程序运行状态
    init.update_status(local_mysql_pool, global_table, execute.gtr_status)
    #输出耗时
    end_time = time.time()
    total_time = (end_time - begin_time) / 60
    logger.info(u"总耗时:%.2f min" % total_time)  # 保留两位小数

#程序注册信号监视和接收
#程序现在有三种退出方式：1、正常运行至结束退出，2、出现警告或者错误提示退出，3、ctrl+c或崩溃退出。后面两种方式是通过signal信号接收后退出
signal.signal(2, __abort_quit)  # 接收中断进程信号(ctrl+c)，暴力退出
signal.signal(15, __abort_quit)  # 接收软件终止信号，被kill（不带-9），暴力退出
signal.signal(14, __normal_quit)  # 接收signal.alarm传递过来的信号14，普通退出，用于程序抛出警告或错误后记录程序状态

#获取日志处理器
logger = log_conf.get_logging("main")

#初始化参数
config = ConfigParser.ConfigParser()
config.read(".config.ini")

#获取repairInfo
global_table = config.get("repairInfo","global_table")
time_column = config.get("repairInfo","time_column")
timr_format = config.get("repairInfo","timr_format")
#获取mysqlInfo
mysql_user = config.get("mysqlInfo","mysql_user")
mysql_password = config.get("mysqlInfo","mysql_password")
mysql_port = config.getint("mysqlInfo","mysql_port")
mysql_host = config.get("mysqlInfo","mysql_host")
mysql_db = config.get("mysqlInfo","mysql_db")
#获取checkInfo
run_mode = config.getint("checkInfo","run_mode")
getid_size = config.getint("checkInfo","getid_size")
chunk_size = config.getint("checkInfo","chunk_size")
chunk_size = chunk_size - 1   #要减去1，因为mysql的limit是从0开始,limit 0,2才是正确，limit 1,2会导致chunk多一条数据
parallel_num = config.getint("checkInfo","parallel_num")
auto_repair_size = config.getint("checkInfo","auto_repair_size")

if chunk_size < 0:
    logger.error(u"chunk_size配置错误")
    sys.exit(0)
if parallel_num < 0:
    logger.error(u"parallel_size配置错误")
    sys.exit(0)
if auto_repair_size < 0:
    logger.error(u"auto_repair_size配置错误")
    sys.exit(0)
if timr_format not in ('str', 'int'):
    logger.error(u"timr_format配置错误")
    sys.exit(0)

#找出datahost信息
#格式：[{'dataHost': {'host': '192.168.147.231', 'port': '3306', 'user': 'root','password':'root'}, 'database': 'db1','datanode':'node1'}, {'dataHost': {'host': '192.168.147.232', 'port': '3306', 'user': 'root','password':'root'}, 'database': 'db2','datanode':'node1'}]
tree = ET.parse('schema.xml')
root = tree.getroot()
datahost_list = []
for row1 in root.findall('dataNode'):
    for row2 in root.findall('dataHost'):
            if row1.attrib['dataHost'] == row2.attrib['name']:
                datahost_list.append({"dataHost": {"host":row2.find('writeHost').attrib['url'].split(":")[0],"port":row2.find('writeHost').attrib['url'].split(":")[1],"user":row2.find('writeHost').attrib['user'],"password":row2.find('writeHost').attrib['password']}, "database": row1.attrib["database"], "datanode": row1.attrib["name"]})
logger.debug('datahost_list add host information:%s' % datahost_list)

#初始化各个节点连接池，节点连接池是基于db的，如果一个mysql有多个db则创建多个连接池；同时因为需要读取大表主键id，所以用流式游标
#node_pool_list格式：[{'database': 'db1','datanode':'node1','pool':node_pool},{'database': 'db2','datanode':'node2','pool',node_pool}]
node_pool_list = []
for row in datahost_list:
    try:
        node_pool = dbpool.getPool(row["dataHost"]["host"],int(row["dataHost"]["port"]),row["dataHost"]["user"],row["dataHost"]["password"],row["database"],2,pymysql.cursors.SSDictCursor)
    except:
        logger.error(u'%s节点无法连接，请检查schema.xml配置' % row["database"])
        sys.exit(1)
    node_pool_list.append({"database":row["database"],"datanode":row["datanode"],"pool":node_pool})

#初始化本地mysql连接池，初始化连接数数量以节点数量为准
try:
    local_mysql_pool = dbpool.getPool(mysql_host,mysql_port,mysql_user,mysql_password,mysql_db,len(datahost_list),pymysql.cursors.SSDictCursor)
except:
    logger.error(u'本地mysql无法连接，请检查config.ini配置')
    sys.exit(1)

#如果cpu数量小于parallel_num，则并发数量以当前cpu数量为准
# if cpu_count() < parallel_num:
#     parallel_num = cpu_count()
#节点数量小于parallel_num，则并发数量以节点数量为准（如果parallel_num大于节点数量，多余的进程会是僵尸进程，也没有作用，反而占用资源）
if len(datahost_list) < parallel_num:
    parallel_num = len(datahost_list)

if run_mode == 0:
    logger.info(u"运行模式：%s（检查并修复）" % run_mode)
elif run_mode ==1:
    logger.info(u"运行模式：%s（只检查）" % run_mode)
elif run_mode ==2:
    logger.info(u"运行模式：%s（只修复）" % run_mode)
elif run_mode ==3:
    logger.info(u"运行模式：%s（断点续检查）" % run_mode)
else:
    logger.error("run_mode:%s is not support" % run_mode)
    sys.exit(0)
begin_time = time.time()
#创建相关表结构
bak_global_table = "bak_" + global_table
primary_key_name,primary_key_type,time_value,unique_column_list = init.check(global_table,node_pool_list,local_mysql_pool,mysql_db,time_column,timr_format,run_mode)
init.create_local_table(global_table,bak_global_table,node_pool_list,local_mysql_pool,mysql_db,primary_key_type)
init.update_status(local_mysql_pool,global_table,0,run_mode)
if run_mode == 0 or run_mode == 1:
    getid.run(global_table,local_mysql_pool,node_pool_list,parallel_num,getid_size,primary_key_name,primary_key_type)
if run_mode == 0 or run_mode == 1 or run_mode == 3:
    checksum.run(global_table,local_mysql_pool,node_pool_list,parallel_num,run_mode,chunk_size,primary_key_name)
if (run_mode == 0 or run_mode == 2):
    sync.run(global_table, local_mysql_pool,node_pool_list,auto_repair_size,bak_global_table,primary_key_name,time_column,time_value,unique_column_list)
end_time = time.time()
total_time = (end_time - begin_time)/60
logger.info(u"总耗时:%.2f min" % total_time)    #保留两位小数
init.update_status(local_mysql_pool,global_table,1)