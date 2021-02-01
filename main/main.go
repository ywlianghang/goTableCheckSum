package main

import (
	"fmt"
	"goProject/dispose"
	"goProject/flag"
    "goProject/CURD"
	"os"
	"strconv"
	"strings"
	"time"
)


func ConnInitCheck()  {   //传参初始化
    var tableList []string
	a := flag.CliHelp()    //获取命令行传参

	if a["status"] == "false"{
		fmt.Println("Incorrect input parameters, please use --help to see the relevant parameters")
		return
	}
	s,d,status := CURD.DSNconn(a)    //获取源目端*sql.db
	if !status{
		fmt.Println("[error]: Failed to connect to the source or destination database. Please confirm whether the current database service is normal and the connection is available.")
		return
	}

	if a["hostPort"] == "false"{
		fmt.Println("[error]: The source or destination connection address cannot be the same. Please modify the source or destination connection address or port.")
		return
	}
	if a["tablename"] == "all" {
		tableList = dispose.DatabaseInitCheck(s,d,a["database"])
		if len(tableList) == 0{
			fmt.Printf("[error]: The source-side database %s or the target-side database %s is empty without any tables\n",a["database"],a["database"])
			return
		}
	}else {
		table := strings.Split(a["tablename"],",")
		for i:= range table{
			tableList = append(tableList,table[i])
		}
	}

	if a["ignoreTable"] != "NULL" {
		var aa []string
		a := strings.Split(a["ignoreTable"],",")
		bmap := make(map[string]int)
		for _,v:= range tableList {
			bmap[v] = 0
		}
		for _,v := range a{
			bmap[v] = 1
		}
		for k,v := range bmap{
			if v == 0{
				aa = append(aa,k)
			}
		}
		tableList = aa
	}
	fmt.Printf(" -- database %s  initialization completes,begin initialization table -- \n",a["database"])
	if a["where"] != "NULL"{
		if len(tableList) >1{
			fmt.Printf("[error]: Use the WHERE function to specify more than just a single table\n")
			return
		}
	}
	for i:= range tableList{  //表预检测
		PRIcolumn,status := dispose.QueryTablePRIColumn(s,a["database"],tableList[i])
		if !status || PRIcolumn == ""{
			fmt.Printf("[error]: The table %s under the current databases %s does not have a primary key, please check whether the table structure has a primary key index, currently only supports int primary key.\n",tableList[i],a["database"])
			return
		}
		rowsCount,status1 := dispose.EmtyTableCheck(s,a["database"],tableList[i])
		if rowsCount ==0 || status1 == false {
			fmt.Printf("[error]: The table %s data in the current database %s is empty. Please login to the database to check the table data.You can ignore this table with the -igt parameter.\n",tableList[i],a["database"])
			return
		}
	}
	for i:= range tableList {
		start := time.Now()
		fmt.Printf(" -- Start initial database %s table %s -- \n",a["database"],tableList[i])
		fmt.Printf(" ** table %s  check start ** \n",tableList[i])
		PRIcolumn,_ := dispose.QueryTablePRIColumn(s,a["database"],tableList[i])
		ab := dispose.TableInfo{
			SourceDB:  s,
			DestDB:    d,
			DBname:    a["database"],
			Tablename: tableList[i],
			PRIcolumn: PRIcolumn,
			DataFix: a["datafix"],
			CheckSum: a["checksum"],
			WhereSql: a["where"],
		}
		cc, _ := strconv.Atoi(a["chunkSize"])
		ac := dispose.ChunkInfo{
			ChunkSize:       cc,
			ChunkSizeStauts: true,
		}
		sqlFile := "/tmp/"+ a["database"] + "_" + tableList[i] + ".sql"
		_,err := os.Stat(sqlFile)
		if err == nil{
			os.Remove(sqlFile)
		}
		if !TableInitCheck(&ab, &ac) {
				break
			}
		end := time.Now()
		curr := end.Sub(start)
		fmt.Printf(" ** table %s check completed ** \n", tableList[i])
		fmt.Printf(" ** check table %s time is %s ** \n",tableList[i],curr)
		fmt.Println()
	}
}

func TableInitCheck(table *dispose.TableInfo,chunk *dispose.ChunkInfo) bool{  //初始化表数据，检查当前表数据大小、chunk的job数量、单个chunk的始末值
	var status bool = true
	if !dispose.ChunkValue(table,chunk){
		status = false
		return status
	}

	if !dispose.ChunkPoint(table,chunk){
		status = false
		return status
	}
	if table.WhereSql != "NULL"{
		dispose.WhereExecSql(table)
	}else {
		dispose.ChanExecSql(table, chunk)
	}
	return status

}

func main(){
	ConnInitCheck()
	//Incremental.QueryBinlogEvent()
}

