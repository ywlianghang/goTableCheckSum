# MySQL非主从环境下数据一致性校验及修复程序

1. 简介
主从环境下数据一致性校验经常会用 pt-table-checksum 工具，它的原理及实施过程之前写过一篇文章：生产环境使用 pt-table-checksum 检查MySQL数据一致性。但是DBA工作中还会有些针对两个表检查是否一致，而这两个表之间并没有主从关系，pt工具是基于binlog把在主库进行的检查动作，在从库重放一遍，此时就不适用了。

在MySQL高可用架构下经常会遇到数据迁移，架构变更的操作，但身为DBA针对此类变更最头疼的就是数据迁移完成后如何确定源目端数据是否一致，在迁移过程中是否有数据遗漏或数据异常。

总会有这样特殊的需求，比如从阿里云RDS实例迁移到自建mysql实例，它的数据传输服务实现方式是基于表的批量数据提取，加上binlog订阅，但强制row模式会导致pt-table-checksum没有权限把会话临时改成statement。另一种需求是，整库进行字符集转换：库表定义都是utf8，但应用连接使用了默认的 latin1，要将连接字符集和表字符集统一起来，只能以latin1导出数据，再以utf8导入，这种情况数据一致性校验，且不说binlog解析程序不支持statement（如canal），新旧库本身内容不同，pt-table-checksum 算出的校验值也会不一样，失效。

2、下载
I also build a bin file. You can download it from here, have a try! I have tested it on centos7.2 and ubuntu 16.04.

Usage

The source and destination specified by goTableCheckSum cannot be the same, and the data can only be checked offline

NAME:

   goTableCheckSum - mysql table data verification

USAGE:

   goTableCheckSum [global options] command [command options] [arguments...]

VERSION:

   1.0.1
   
COMMANDS:

   help, h  Shows a list of commands or help for one command
   
GLOBAL OPTIONS:

--source value, -s value            Source side database connection information. For example: --source host=127.0.0.1,user=root,password=abc123,P=3306 (default: "NULL")

--dest value, -d value              Target database connection information. For example: --dest host=127.0.0.1,user=root,password=abc123,P=3306 (default: "NULL")

   --database value, -D value          checksum Database name. For example: -D pcms or -D=pcms (default: "NULL")
   
   --table value, -t value             checksum table name. For example: --table=a, or --table=a,b... (default: "all")
   
   --ignoreTable value, --igt value    Ignore a check for a table. For example: --igt=a or --igt=a,b... (default: "NULL")
   
   --character value, --charset value  connection database character. For example: --charset=utf8 (default: "utf8")
   
   --chunkSize value, --chunk value    How many rows of data are checked at a time. For example: --chunk=1000 or --chunk 1000 (default: "10000")
   
   --datafix value                     Fix SQL written to a file or executed directly. For example: --datafix=file or --datafix file (default: "file")
   
   --help, -h                          show help
   
   --version, -v                       print the version


Examples



Building

goTableChecksum needs go version > 1.12 for go mod

sehll> git clone https://github.com/silenceshell/hcache.git

shell> cd main

shell> go build -o goTableChecksum main.go

shell> chmod +x goTableChecksum

shell> mv goTableChecksum /usr/bin

6、要求

7、功能需求

8、作者
