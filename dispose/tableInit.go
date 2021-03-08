package dispose

import (
	"fmt"
	"goProject/flag"
	"os"
	"strconv"
	"strings"
	"time"
)
func TableInitCheck(table *TableInfo,chunk *ChunkInfo) bool{ //初始化表数据，检查当前表数据大小、chunk的job数量、单个chunk的始末值
	var status bool = true
	if !ChunkValue(table,chunk){
		status = false
		return status
	}

	if !ChunkPoint(table,chunk){
		status = false
		return status
	}

	if table.WhereSql != "NULL"{
		WhereExecSql(table)
	}else {
		ChanExecSql(table, chunk)
	}
	return status
}


func ConnInitCheck(o *flag.ConnParameter){ //传参初始化
	flag.ParameterLimits(o)
	if !o.HelpStatus {
		return
	}
	DSNconn(o) //获取源目端*sql.db
	for k, v := range o.Dbdirct {
		if strings.Index(k, "oracle") != -1 {
			if string(k[0]) == "s" {
				o.SdatabaseType = "oracle"
				o.Sdb = v
			} else {
				o.DdatabaseType = "oracle"
				o.Ddb = v
			}
		}
		if strings.Index(k, "mysql") != -1 {
			if string(k[0]) == "s" {
				o.SdatabaseType = "mysql"
				o.Sdb = v
			} else {
				o.DdatabaseType = "mysql"
				o.Ddb = v
			}
		}
	}

	//判断需要数据校验的表有哪些
	if o.Tablename == "ALL" {
		o.TableList = DatabaseInitCheck(o.SdatabaseType, o.DdatabaseType, o.Sdb, o.Ddb, o.Database)
		if len(o.TableList) == 0 {
			fmt.Printf("[error]: The source-side database %s or the target-side database %s is empty without any tables\n", o.Database, o.Database)
			return
		}
	} else {
		table := strings.Split(o.Tablename, ",")
		for i := range table {
			o.TableList = append(o.TableList, strings.ToUpper(table[i]))
		}
	}

	// 是否忽略某个表的数据校验
	if o.IgnoreTable != "NULL" {
		var aa []string
		o.IgnoreTable = strings.ToUpper(o.IgnoreTable)
		a := strings.Split(o.IgnoreTable, ",")
		bmap := make(map[string]int)
		for _, v := range o.TableList {
			bmap[v] = 0
		}
		for _, v := range a {
			bmap[v] = 1
		}
		for k, v := range bmap {
			if v == 0 {
				aa = append(aa, k)
			}
		}
		o.TableList = aa
	}


	fmt.Printf(" -- database %s  initialization completes,begin initialization table -- \n", o.Database)
	//针对要校验的表进行与检查，检查是否有主键，是否有数据（空表）
	for i := range o.TableList { //表预检测
		PRIcolumn, status := QueryTablePRIColumn(o.SdatabaseType, o.Sdb, o.Database, o.TableList[i])
		if !status || PRIcolumn == "" {
			fmt.Printf("[error]: The table %s under the current databases %s does not have a primary key, please check whether the table structure has a primary key index, currently only supports int primary key.\n", o.TableList[i], o.Database)
            o.CheckTableStatus = false
			return
		}
		rowsCount, status1 := EmtyTableCheck(o.SdatabaseType, o.Sdb, o.Database, o.TableList[i])
		if rowsCount == 0 || status1 == false {
			fmt.Printf("[error]: The table %s data in the current database %s is empty. Please login to the database to check the table data.You can ignore this table with the -igt parameter.\n", o.TableList[i], o.Database)
			o.CheckTableStatus = false
			return
		}
	}
}

func TableCheckActive(){
	var p flag.ConnParameter
	ConnInitCheck(&p)
	if p.CheckTableStatus {
		for i := range p.TableList {
			start := time.Now()
			fmt.Printf(" -- Start initial database %s table %s -- \n", p.Database, p.TableList[i])
			fmt.Printf(" ** table %s  check start ** \n", p.TableList[i])
			PRIcolumn, _ := QueryTablePRIColumn(p.SdatabaseType, p.Sdb, p.Database, p.TableList[i])

			ab := TableInfo{
				SourceDB:      p.Sdb,
				DestDB:        p.Ddb,
				DBname:        p.Database,
				Tablename:     p.TableList[i],
				PRIcolumn:     PRIcolumn,
				DataFix:       p.Datafix,
				CheckSum:      p.CheckSum,
				WhereSql:      p.Where,
				SdatabaseType: p.SdatabaseType,
				DdatabaseType: p.DdatabaseType,
			}
			cc, _ := strconv.Atoi(p.ChunkSize)
			ac := ChunkInfo{
				ChunkSize:       cc,
				ChunkSizeStauts: true,
			}
			sqlFile := "/tmp/"+ p.Database + "_" + p.TableList[i] + ".sql"
			//sqlFile := "C:\\" + p.Database + "_" + p.TableList[i] + ".sql"
			_, err := os.Stat(sqlFile)
			if err == nil {
				os.Remove(sqlFile)
			}
			if !TableInitCheck(&ab, &ac) {
				break
			}
			end := time.Now()
			curr := end.Sub(start)
			fmt.Printf(" ** table %s check completed ** \n", p.TableList[i])
			fmt.Printf(" ** check table %s time is %s ** \n", p.TableList[i], curr)
			fmt.Println()
		}
	}
}