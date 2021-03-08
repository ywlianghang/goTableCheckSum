package flag

import (
	"database/sql"
	"fmt"
	"github.com/urfave/cli"
	"os"
	"strings"
)

type ConnParameter struct {
	SourceConnection, DestConnection, Database, Charset, Datafix, ChunkSize, Tablename, IgnoreTable, CheckSum, Where, FrameworkCode, OracleSid string
	Suser,Spassword,Shost,Sport,Duser,Dpassword,Dhost,Dport string
	Dbdirct map[string]*sql.DB
	SdatabaseType,DdatabaseType string
	TableList []string
	Sdb,Ddb *sql.DB
	CheckTableStatus,HelpStatus bool
}

func CliHelp(q *ConnParameter){
	app := cli.NewApp()
	app.Name = "goTableCheckSum"                         //应用名称
	app.Usage = "mysql Oracle table data verification" //应用功能说明
	app.Author = "lianghang"                           //作者
	app.Email = "ywlianghang@gmail.com"                //邮箱
	app.Version = "1.1.1"                              //版本
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "frameworkCode,f", //命令名称
			Usage: "The type of the current validation schema. for example: MySQL(source) <-> MySQL(dest) is -f mm or -f=mm," +
				"Oracle(source) <-> MySQL(dest) is -f om or -f=om", //命令说明
			Value:       "mm",                                                                                  //默认值
			Destination: &q.FrameworkCode,                                                                        //赋值
		},
		cli.StringFlag{
			Name:        "OracleSid,osid",                                                                                      //命令名称
			Usage:       "The SID required to connect to Oracle. for example：SID is \"helowin\", -sid helowin or -sid=helowin", //命令说明
			Value:       "NULL",                                                                                                //默认值
			Destination: &q.OracleSid,                                                                                            //赋值
		},
		cli.StringFlag{
			Name:        "source,s",                                                                                                           //命令名称
			Usage:       "Source side database connection information. For example: --source host=127.0.0.1,user=root,password=abc123,P=3306", //命令说明
			Value:       "NULL",                                                                                                               //默认值
			Destination: &q.SourceConnection,                                                                                                    //赋值
		},
		cli.StringFlag{
			Name:        "dest,d",                                                                                                      //命令名称
			Usage:       "Target database connection information. For example: --dest host=127.0.0.1,user=root,password=abc123,P=3306", //命令说明
			Value:       "NULL",                                                                                                        //默认值
			Destination: &q.DestConnection,                                                                                               //赋值
		},
		cli.StringFlag{
			Name:        "database,D",                                              //命令名称
			Usage:       "checksum Database name. For example: -D pcms or -D=pcms", //命令说明
			Value:       "NULL",                                                    //默认值
			Destination: &q.Database,                                                 //赋值
		},
		cli.StringFlag{
			Name:        "table,t",                                                        //命令名称
			Usage:       "checksum table name. For example: --table=a, or --table=a,b...", //命令说明
			Value:       "ALL",                                                            //默认值
			Destination: &q.Tablename,                                                       //赋值
		},
		cli.StringFlag{
			Name:        "ignoreTable,igt",                                                  //命令名称
			Usage:       "Ignore a check for a table. For example: --igt=a or --igt=a,b...", //命令说明
			Value:       "NULL",                                                             //默认值
			Destination: &q.IgnoreTable,                                                       //赋值
		},
		cli.StringFlag{
			Name:        "character,charset",                                          //命令名称
			Usage:       "connection database character. For example: --charset=utf8", //命令说明
			Value:       "utf8",                                                       //默认值
			Destination: &q.Charset,                                                     //赋值
		},
		cli.StringFlag{
			Name:        "chunkSize,chunk",                                                                        //命令名称
			Usage:       "How many rows of data are checked at a time. For example: --chunk=1000 or --chunk 1000", //命令说明
			Value:       "10000",                                                                                  //默认值
			Destination: &q.ChunkSize,                                                                               //赋值
		},
		cli.StringFlag{
			Name:        "datafix",                                                                                        //命令名称
			Usage:       "Fix SQL written to a file or executed directly. For example: --datafix=file or --datafix table", //命令说明
			Value:       "file",                                                                                           //默认值
			Destination: &q.Datafix,                                                                                         //赋值
		},
		cli.StringFlag{
			Name:        "checksum,cks",                                                                                                                                                   //命令名称
			Usage:       "Specifies algorithms for source and target-side data validation.values are CRC32 MD5 SHA1. For example: --checksum=CRC32 or --checksum MD5 or --checksum=HASH1", //命令说明
			Value:       "CRC32",                                                                                                                                                          //默认值
			Destination: &q.CheckSum,                                                                                                                                                        //赋值
		},
		cli.StringFlag{
			Name:        "where",                                                                                                                     //命令名称
			Usage:       "The WHERE condition is used for data validation under a single table. For example: --where \"1=1\" or --where \"id >=10\"", //命令说明
			Value:       "NULL",                                                                                                                      //默认值
			Destination: &q.Where,                                                                                                                      //赋值
		},
	}
	app.Action = func(c *cli.Context) { //应用执行函数
	}
	app.Run(os.Args)
	q.CheckTableStatus = true
	q.HelpStatus = true
	aa := os.Args
    for i:= range aa{
    	if aa[i] == "--help" || aa[i] == "-h"{
            q.HelpStatus = false
		}
	}

}
func ParameterLimits(q *ConnParameter) {
	CliHelp(q)
	if !q.HelpStatus {
		return
	}
	q.Dbdirct = make(map[string]*sql.DB)
	var parametersList string = "crc32,md5,sha1,table,file,mm,om,mo"
	if strings.Index(parametersList,strings.ToLower(q.FrameworkCode)) == -1  {
		fmt.Println("Incorrect frameworkCode input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}

	if q.SourceConnection  != "NULL" {
		ab := strings.Split(q.SourceConnection, ",")
		for i:= range ab{
			if strings.Index(ab[i],"user=") != -1{
				q.Suser = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"host=") != -1{
				q.Shost = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"password=") != -1{
				q.Spassword = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"P=") != -1{
				q.Sport = strings.Split(ab[i],"=")[1]
			}
		}
	}else {
		fmt.Println("Incorrect source database connection input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}

	if q.DestConnection != "NULL" {
		ab := strings.Split(q.DestConnection, ",")
		for i:= range ab{
			if strings.Index(ab[i],"user=") != -1{
				q.Duser = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"host=") != -1{
				q.Dhost = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"password=") != -1{
				q.Dpassword = strings.Split(ab[i],"=")[1]
			}
			if strings.Index(ab[i],"P=") != -1{
				q.Dport = strings.Split(ab[i],"=")[1]
			}
		}
	}else{
		fmt.Println("Incorrect dest database connection input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}
	if q.Database == "NULL"{
		fmt.Println("Incorrect database name input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}
	if q.Shost == q.Dhost && q.Sport == q.Dport{
		fmt.Println("Incorrect Same source end connection address (host and port) input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}
	if strings.Index(parametersList,strings.ToLower(q.CheckSum)) == -1 {
		fmt.Println("Incorrect CheckSum algorithm input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}
	if strings.Index(parametersList,strings.ToLower(q.Datafix)) == -1  {
		fmt.Println("Incorrect datafix input parameters, please use --help to see the relevant parameters")
		q.HelpStatus = false
		return
	}

	//支持单表下使用where条件进行数据过滤校验
	if q.Where != "NULL" { //限制使用where条件的表数量，默认支持单表，多表直接退出
		if q.Tablename == "ALL"{
			fmt.Printf("[error]: Use the WHERE function to specify more than just a single table\n")
			q.HelpStatus = false
			return
		}else if len(strings.Split(q.Tablename,",")) >1 {
			fmt.Printf("[error]: Use the WHERE function to specify more than just a single table\n")
			q.HelpStatus = false
			return
		}
	}
}

