package flag

import (
	"github.com/urfave/cli"
	"os"
	"strings"
)

func CliHelp() map[string]string{
	var sourceConnection string
	var destConnection string
	var database string
	var charset string
	var datafix string
	var chunkSize string
	var tablename string
	var ignoreTable string
    var CheckSum string
    var where string
	var parametersList string = "crc32,md5,sha1,table,file"

	aa := make(map[string]string)
	app := cli.NewApp()
	app.Name = "TableCheckSum"                  //应用名称
	app.Usage = "mysql table data verification" //应用功能说明
	app.Author = "lianghang"                    //作者
	app.Email = "ywlianghang@gmail.com"         //邮箱
	app.Version = "1.0.1"                       //版本
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name :"source,s",    //命令名称
			Usage: "Source side database connection information. For example: --source host=127.0.0.1,user=root,password=abc123,P=3306",  //命令说明
			Value: "NULL",         //默认值
			Destination: &sourceConnection,  //赋值
		},
		cli.StringFlag{
			Name :"dest,d",    //命令名称
			Usage: "Target database connection information. For example: --dest host=127.0.0.1,user=root,password=abc123,P=3306",  //命令说明
			Value: "NULL",         //默认值
			Destination: &destConnection,  //赋值
		},
		cli.StringFlag{
			Name :"database,D",    //命令名称
			Usage: "checksum Database name. For example: -D pcms or -D=pcms",  //命令说明
			Value: "NULL",         //默认值
			Destination: &database,  //赋值
		},
		cli.StringFlag{
			Name :"table,t",    //命令名称
			Usage: "checksum table name. For example: --table=a, or --table=a,b...",  //命令说明
			Value: "all",         //默认值
			Destination: &tablename,  //赋值
		},
		cli.StringFlag{
			Name :"ignoreTable,igt",    //命令名称
			Usage: "Ignore a check for a table. For example: --igt=a or --igt=a,b...",  //命令说明
			Value: "NULL",         //默认值
			Destination: &ignoreTable,  //赋值
		},
		cli.StringFlag{
			Name :"character,charset",    //命令名称
			Usage: "connection database character. For example: --charset=utf8",  //命令说明
			Value: "utf8",         //默认值
			Destination: &charset,  //赋值
		},
		cli.StringFlag{
			Name :"chunkSize,chunk",    //命令名称
			Usage: "How many rows of data are checked at a time. For example: --chunk=1000 or --chunk 1000",  //命令说明
			Value: "10000",         //默认值
			Destination: &chunkSize,  //赋值
		},
		cli.StringFlag{
			Name :"datafix",    //命令名称
			Usage: "Fix SQL written to a file or executed directly. For example: --datafix=file or --datafix table" ,  //命令说明
			Value: "file",         //默认值
			Destination: &datafix,  //赋值
		},
		cli.StringFlag{
			Name :"checksum,cks",    //命令名称
			Usage: "Specifies algorithms for source and target-side data validation.values are CRC32 MD5 SHA1. For example: --checksum=CRC32 or --checksum MD5 or --checksum=HASH1" ,  //命令说明
			Value: "CRC32",         //默认值
			Destination: &CheckSum,  //赋值
		},
		cli.StringFlag{
			Name :"where",    //命令名称
			Usage: "The WHERE condition is used for data validation under a single table. For example: --where \"1=1\" or --where \"id >=10\"" ,  //命令说明
			Value: "NULL",         //默认值
			Destination: &where,  //赋值
		},
	}
	app.Action = func(c *cli.Context) {  //应用执行函数
	}
	app.Run(os.Args)
	aa["datafix"] = datafix
	aa["charset"] = charset
	aa["chunkSize"] = chunkSize
	aa["tablename"] = tablename
	aa["ignoreTable"] = ignoreTable
	aa["database"] = database
	aa["checksum"] = CheckSum
	aa["where"] = where

	if sourceConnection  != "NULL" {
		ab := strings.Split(sourceConnection, ",")
		for i:= range ab{
			if strings.Index(ab[i],"user=") != -1{
				aa["suser"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"host=") != -1{
				aa["shost"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"password=") != -1{
				aa["spassword"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"P=") != -1{
				aa["sport"] = strings.Split(ab[i],"=")[1]
			}
		}
	}else {
		aa["status"] = "false"
	}
	if destConnection != "NULL" {
		ab := strings.Split(destConnection, ",")
		for i:= range ab{
			if strings.Index(ab[i],"user=") != -1{
				aa["duser"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"host=") != -1{
				aa["dhost"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"password=") != -1{
				aa["dpassword"] = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"P=") != -1{
				aa["dport"] = strings.Split(ab[i],"=")[1]
			}
		}
	}else{
		aa["status"] = "false"
	}
	if database != "NULL"{
		aa["database"] = database
	}else {
		aa["status"] = "false"
	}
	if aa["shost"] == aa["dhost"] && aa["sport"] == aa["dport"]{
		aa["hostPort"] = "false"
	}
	if strings.Index(parametersList,strings.ToLower(CheckSum)) == -1 {
		aa["status"] = "false"
	}
	if strings.Index(parametersList,strings.ToLower(datafix)) == -1  {
		aa["status"] = "false"
	}
	return aa
}
