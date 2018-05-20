

程序作用：   
1、mycat全局表不一致校验和修复  
2、也可以用于主从复制检查


环境要求：   
1、在schema.xml中配置db节点，在config.ini中配置相关参数     
2、python 2.6.6，其他版本未测试    
3、全局表表必须要有主键，主键必须只有一个字段     
4、依赖库：DBUtils、pymysql 

部署运行：   
1、安装依赖库：DBUtils、pymysql 
2、把工具拷贝到linux服务器  
3、在mycat_gtr本地安装mysql，my.cnf配置innodb_buffer_pool_size至少1G，关闭binlog  
4、配置config.ini、schema.xml，具体配置项有说明  
5、通过sh mycat_gtr.sh运行 
6、自动修复建议在停机后操作  

表结构说明：    
global_table_id       #存储全局表id，数据为【临时】存储，下次校验时会被清空  
global_table_chunk      #把global_table_id分成多个chunk去检查，检查结果存放到该表，数据为【临时】存储，下次校验时会被清空       
global_table_chunk_differ   #有差异的chunk存放到该表，数据为【临时】存储，下次校验时会被清空     
global_table_status   #记录程序运行记录和状态，数据为【永久性】存储，下次校验时不会被清空  
global_table_id_differ  #对有差异的chunk根据id进行检查，差异id存放到该表，数据为【永久性】存储，下次校验时不会被清空     
bak_table_name    #修复前的备份表，数据为【永久性】存储，下次校验时不会被清空  


详情请查看【mycat_gtr\doc\mycat_gtr详细说明.docx】


tips    
@Author: tanzh(qq:405924341)  
@Inspired by eric.cheng(qq:4897393) 
