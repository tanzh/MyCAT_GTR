#!/bin/bash
#encode begin
  #                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
#endcode end
#complie=true/false
#
#Author: tanzh
#Date: 20180112
#Description:
#Usage:
source /etc/profile

#执行sql语句
function executeSql()
{
        local sql="$*"
        if [ -z "$mysqlUser" -o "$mysqlUser" = "" -o -z "${mysqlPwd}" -o "${mysqlPwd}" = "" ]; then
                echo "[`date +%F' '%T`] mysql user or mysql password is not vaild."
        fi
        if [ "$sql" = "" ]
        then
                echo "[`date +%F' '%T`] sql statement is null "
        else
                echo -e "$sql" | mysql --defaults-extra-file=${my_cnf}  $useDBName --default-character-set=utf8 -N
                if [ "$?" != "0" ] ;then
                    echo "[`date +%F' '%T`] execute sql error:${sql}"
                fi
        fi
}

#检查指定文件是否存在
function checkFileExist()
{
        theFileName="$1"
        if [ ! -f $theFileName ]; then
                echo "[`date +%F' '%T`] The file '$theFileName' is not exist."
        fi
}

#创建mycnf文件，供executeSql使用
function createMycnfFile()
{
        #mysqlHost="localhost"
        mysqlPort="$1"
        mysqlUser="$2"
        mysqlPwd="$3"
        mysqlHost="$4"
        mysqlPid=`netstat -naltp | grep 'LISTEN' | awk   '$4~/'"$mysqlPort"'/{print $0}' | awk '{print $NF}' | awk -F'/' '{print $1}'`
        mysqlSock=`netstat -nap | grep "${mysqlPid}" | grep 'LISTENING' | awk '{print $NF}'`
        echo -e "\n[client]\nhost=${mysqlHost}\nuser=${mysqlUser}\npassword='${mysqlPwd}'\nport=${mysqlPort}\nsocket=${mysqlSock}" > ${my_cnf}
        echo -e "\n[mysqldump]\nhost=${mysqlHost}\nuser=${mysqlUser}\npassword='${mysqlPwd}'\nport=${mysqlPort}\nsocket=${mysqlSock}" >> ${my_cnf}
}

function shellInit()
{
        shellPId="$$"
        my_cnf="/tmp/.my_${shellPId}.cnf"
        #退出/中止（包括导常退出）脚本时执行的命令
        trapCmd="rm -f /tmp/.my_${shellPId}.cnf"
        trap 'eval ${trapCmd}' exit
        #--------以上为框架规定，建议不要做修改----------
        createMycnfFile "${mysqlPort}" "${mysqlUser}" "${mysqlPwd}" "${mysqlHost}"
}

function check()
{
        checkFileExist "config.ini"
        checkFileExist "schema.xml"
}

#使用说明
function usage()
{
        parameter="$1"
        myFileName=`basename $0`
        if [ "${parameter}" = '--help'  -o  "${parameter}" = '-h' ]; then
                echo "Usage:  sh ${myFileName} \${parameter_1}"
                exit 1
        fi
}

function main()
{
        check
        #找出数据库密码
        mysqlUser=`cat config.ini | grep  mysql_user= | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        mysqlPwd=`cat config.ini | grep  mysql_password= | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        mysqlPort=`cat config.ini | grep  mysql_port= | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        mysqlHost=`cat config.ini | grep  mysql_host= | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        mycat_gtr_log=`cat config.ini | grep  log_name= | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        shellInit
        #拷贝出一份隐式配置文件
        \cp -rf config.ini .config.ini
        #找出存储数据库并创建
        database=`cat config.ini | grep -w 'mysql_db'| awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        sql="create database if not exists ${database}"
        executeSql ${sql}
        #找出要修复的表
        repair_table_list=`cat config.ini | grep -w 'repair_table_list' | awk -F '=' '{print $2}'|sed 's/ *$//g'|sed 's/^ *//g'`
        repair_count=`echo ${repair_table_list} | grep -o ',' | wc -l`
        repair_count=$[ ${repair_count} + 1 ]
        count=1
        while [ ${count} -le ${repair_count} ] ; do
            repair_table=`echo ${repair_table_list} | awk -F ',' "{print \$"${count}"}"`
            echo "[`date +%F' '%T`] 开始校验【${repair_table}】===========================" | tee -a ${mycat_gtr_log}
            sed -i  "/global_table=/d" .config.ini
            sed -i  "/repair_table_list=/i  global_table=${repair_table}" .config.ini
            count=$[ $count + 1 ]
            python ./mycat_gtr/main.py
            #判断是否程序发生错误，gtr程序中存在sys.exit(0)，这种不应该退出循环而只是跳过当前表的检查继续下一张表
            if [ "$?" != "0" ];then
                break
            fi
        done
}
main