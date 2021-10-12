# MySQL非主从环境下数据一致性校验及修复程序 #

----------
##  Introductory ##

    主从环境下数据一致性校验经常会用pt-table-checksum工具。但是pt-table-checksum工具对环境依赖较高，
    需要正常的主从环境，这对非主从环境的数据校验就不适用了，但身为DBA针对此类变更最头疼的就是数据迁移
    完成后如何确定源目端数据是否一致，在迁移过程中是否有数据遗漏或数据异常。于是萌发了自己使用go写一个
    离线数据校验工具的想法。
    
    1）测试 pt-table-checksum 的缺陷如下：
    （1）pt-table-checksum 数据校验使用数据库的CRC32进行校验，针对表中两个列名及数据相同但顺序不同，无法检测出来
    （2）pt-table-checksum 依赖主从关系，非主从关系的数据库无法检测
    （3）pt-table-checksum 是基于binlog把在主库进行的检查动作，在从库重放一遍
    （4）pt-table-checksum 需要安装依赖包，针对平台有限制
     
    2）goTableCheckSum 针对 pt-table-checksum 的缺陷改造：
    （1）goTableCheckSum 数据校验是有程序进行校验，可以使用CRC32、MD5、SHA1算法进行对数据校验，将数据使用字节流的形式查询校验，规避上述问题
    （2）goTableCheckSum 对源目端数据库只会执行select查询数据操作，对源目端的数据库产生的压力较小
    （3）goTableCheckSum 支持针对指定的单表或多表进行数据校验、可以指定忽略校验的表
    （4）goTableCheckSum 支持指定where条件的数据校验查询，但仅限于单表
    （5）goTableCheckSum 支持自定义每次检验数据库的chunk大小。即每次校验多少条行数
    （6）goTableCheckSum 支持自定义修复语句的执行方式，是写在文件中还是直接在表中执行
    （7）goTableCheckSum 支持针对单表的where条件的数据校验
    （8）goTableCheckSum 支持指定检验数据算法CRC32、MD5、HASH1
    （9）goTableCheckSum 支持MySQL <-> MySQL、Oracle <-> MySQL之间的异构数据校验

    3）goTableCheckSum 后续功能更新
    （1）增加相关日志输出（debug、info、warning、error）
    （2）针对数据校验增加并发操作，降低数据校验时长
    （3）增加单表的自定义列数据校验
    （4）断点校验
    （5）增加针对源库数据库压力的监控，当压力高时自动减少并发及chunk数据量，或暂停校验
    （6）增加根据gitd信息读取binlog日志，实现增量数据的校验

------

## Download  ##

   你可以从 [这里](https://github.com/ywlianghang/goTableCheckSum/releases) 下载二进制可执行文件，我已经在ubuntu、centos、redhat、windows x64下测试过

-----
## Usage  ##

   假如需要校验oracle数据库，则需要下载oracle相应版本的驱动，例如：待校验的数据库为11-2则需要去下载11-2的驱动,并生效,否则连接Oracle会报错

   安装Oracle Instant Client

    从https://www.oracle.com/database/technologies/instant-client/downloads.html下载免费的Basic或Basic Light软件包。
    # oracle basic client
    instantclient-basic-linux.x64-11.2.0.4.0.zip
    # oracle sqlplus
    instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
    # oracle sdk
    instantclient-sdk-linux.x64-11.2.0.4.0.zip

   配置oracle client并生效

    shell> unzip instantclient-basic-linux.x64-11.2.0.4.0.zip
    shell> unzip instantclient-sqlplus-linux.x64-11.2.0.4.0.zip
    shell> unzip instantclient-sdk-linux.x64-11.2.0.4.0.zip
    shell> mv instantclient_11_2 /usr/local
    shell> echo "export LD_LIBRARY_PATH=/usr/local/instantclient_11_2:$LD_LIBRARY_PATH" >>/etc/profile
    shell> source /etc/profile

   工具使用说明

    The source and destination specified by goTableCheckSum cannot be the same, and the data can only be checked offline
      NAME:
            goTableCheckSum - mysql table data verification
      USAGE:
            goTableCheckSum [global options] command [command options] [arguments...]
      VERSION:
            2.1.1
      AUTHOR:
            lianghang <ywlianghang@gmail.com>
      COMMANDS:
            help, h  Shows a list of commands or help for one command
      GLOBAL OPTIONS:
            --frameworkCode value, -f value     The type of the current validation schema. for example: MySQL(source) <-> MySQL(dest) is -f mm or -f=mm,Oracle(source) <-> MySQL(dest) is -f om or -f=om, MySQL(source) <-> Oracle(dest) is -f mo or -f=mo (default: "mm")
            --OracleSid value, --osid value     The SID required to connect to Oracle. for example：SID is "helowin", -sid helowin or -sid=helowin (default: "NULL")
            --source value, -s value            Source side database connection information. For example: --source host=127.0.0.1,user=root,password=abc123,P=3306 (default:"NULL")
            --dest value, -d value              Target database connection information. For example: --dest host=127.0.0.1,user=root,password=abc123,P=3306 (default: "NULL")
            --database value, -D value          checksum Database name. For example: -D pcms or -D=pcms (default: "NULL")
            --table value, -t value             checksum table name. For example: --table=a, or --table=a,b... (default: "ALL")
            --ignoreTable value, --igt value    Ignore a check for a table. For example: --igt=a or --igt=a,b... (default: "NULL")
            --character value, --charset value  connection database character. For example: --charset=utf8 (default: "utf8")
            --chunkSize value, --chunk value    How many rows of data are checked at a time. For example: --chunk=1000 or --chunk 1000 (default: "10000")
            --datafix value                     Fix SQL written to a file or executed directly. For example: --datafix=file or --datafix table (default: "file")
            --checksum value, --cks value       Specifies algorithms for source and target-side data validation.values are CRC32 MD5 SHA1. For example: --checksum=CRC32 or
            --checksum MD5 or --checksum=HASH1 (default: "CRC32")
            --where value                       The WHERE condition is used for data validation under a single table. For example: --where "1=1" or --where "id >=10" (default: "NULL")
            --help, -h                          show help
            --version, -v                       print the version


--------
## Examples ##

     1)检测单表数据是否相同，不同则产生修复SQL,默认将修复语句写入到/tmp/目录下，以库名_表名.sql为文件名
     shell> ./goTableCheckSum -s host=172.16.50.161,user=pcms,password=pcms@123,P=3306 -d 
     host=172.16.50.162,user=pcms,password=pcms@123,P=3306 -D pcms -t gobench1
    
     -- database pcms  initialization completes,begin initialization table -- 
     -- Start initial database pcms table gobench1 -- 
     ** table gobench1  check start ** 
     Start the repair Delete SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Insert SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Delete SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Insert SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Delete SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Insert SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     ** table gobench1 check completed ** 
     ** check table gobench1 time is 1.483941625s ** 
     
     2）检测单库pcms下所有表的数据是否相同，不同则产生修复SQL，并直接在目标库中执行
     shell> ./goTableCheckSum -s host=172.16.50.161,user=pcms,password=pcms@123,P=3306 -d 
     host=172.16.50.162,user=pcms,password=pcms@123,P=3306 -D pcms  -datafix table

     -- database pcms  initialization completes,begin initialization table -- 
     -- Start initial database pcms table gobench1 -- 
     ** table gobench1  check start ** 
     Start executing Delete SQL statements in the target databases pcms
     Start executing Insert SQL statements in the target databases pcms
     Start executing Delete SQL statements in the target databases pcms
     Start executing Insert SQL statements in the target databases pcms
     Start executing Delete SQL statements in the target databases pcms
     Start executing Insert SQL statements in the target databases pcms
     ** table gobench1 check completed ** 
     ** check table gobench1 time is 1.633451665s **

     3)检测单表下where条件的数据是否相同，不同则产生修复SQL，并在/tmp/目录下生成修复文件
     shell> ./goTableCheckSum -s host=172.16.50.161,user=pcms,password=pcms@123,P=3306 -d 
     host=172.16.50.162,user=pcms,password=pcms@123,P=3306 -D pcms -t gobench1 -datafix file 
     --where "id <=200000"
     
     -- database pcms  initialization completes,begin initialization table -- 
     -- Start initial database pcms table gobench1 -- 
     ** table gobench1  check start ** 
     Start the repair Delete SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     Start the repair Insert SQL and write the repair SQL to /tmp/pcms_gobench1.sql
     ** table gobench1 check completed ** 
     ** check table gobench1 time is 1.836164054s ** 
    
     4）检测Oracle和MySQL异构下数据是否相同，不同则产生修复SQL，并在/tmp/目录下生成修复文件
     shell> ./goTableCheckSum  -f om -osid helowin -s host=172.16.50.161,user=pcms,password=pcms,P=1521 -d host=172.16.50.161,user=pcms,password=pcms@123,P=3306 -D pcms
     godror WARNING: discrepancy between DBTIMEZONE ("+00:00"=0) and SYSTIMESTAMP ("+08:00"=800) - set connection timezone, see https://github.com/godror/godror/blob/master/doc/timezone.md
     -- database pcms  initialization completes,begin initialization table --
     -- Start initial database pcms table GOBENCH1 --
     ** table GOBENCH1  check start **
     Start the repair Delete SQL and write the repair SQL to /tmp/pcms_GOBENCH1.sql
     Start the repair Insert SQL and write the repair SQL to /tmp/pcms_GOBENCH1.sql
     ** table GOBENCH1 check completed **
     ** check table GOBENCH1 time is 1m25.9036516s **

     -- Start initial database pcms table GOBENCH2 --
     ** table GOBENCH2  check start **
     ** table GOBENCH2 check completed **
     ** check table GOBENCH2 time is 56.8436ms **
     

     



-------
## Building ##

    goTableChecksum needs go version > 1.12 for go mod
     
    shell> git clone https://github.com/ywlianghang/goTableCheckSum.git
    shell> cd main
    shell> go build -o goTableChecksum main.go
    shell> chmod +x goTableChecksum
    shell> mv goTableChecksum /usr/bin

-----
## Requirements ##

    1）待检验表必须有数据，如果没有数据可以使用 --igt 参数忽略该表
    2）待检验表必须有主键
    3）待检验表主键索引必须是int类型，varchar类型暂不支持
    4）MySQL必须开启lower_case_table_names=1
-----
## Author ##

lianghang  ywlianghang@gmail.com
