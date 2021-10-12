package dispose

import (
	"fmt"
	"goProject/PublicFunc"
	"goProject/checksum"
	"goProject/flag"
	mgorm "goProject/mgorm/ExecQuerySQL"
	"os"
	"strconv"
	"strings"
	"time"
)

type sourceMesh struct{
	source string
	dest string
}
type Table struct{
	tablename []sourceMesh
}

func ConnInitCheck(o *flag.ConnParameter,b *mgorm.SummaryInfo) map[string]*mgorm.Connection { //传参初始化
	//判断需要数据校验的表有哪些
	flag.ParameterLimits(o)
	a := make(map[string]*mgorm.Connection)
	var sourceConn, destConn mgorm.Connection
	if o.FrameworkCode[0] == 'm' {
		DNS := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", o.Suser, o.Spassword, o.Shost, o.Sport,o.Database, o.Charset)
		sourceConn = mgorm.Connection{
			DriverName:      "mysql",
			DataSourceName:  DNS,
			MaxIdleConns:    10,
			MaxOpenConns:    50,
			ConnMaxLifetime: 0,
			ConnMaxIdleTime: 0,}
	} else {
		DSN := fmt.Sprintf("user=%s password=%s connectString=%s:%s/%s", o.Suser, o.Spassword, o.Shost, o.Sport, o.OracleSid)
		sourceConn = mgorm.Connection{
			DriverName:      "godror",
			DataSourceName:  DSN,
			MaxIdleConns:    10,
			MaxOpenConns:    50,
			ConnMaxLifetime: 0,
			ConnMaxIdleTime: 0,
		}
	}
	if o.FrameworkCode[1] == 'o' {
		DSN := fmt.Sprintf("user=%s password=%s connectString=%s:%s/%s", o.Duser, o.Dpassword, o.Dhost, o.Dport, o.OracleSid)
		destConn = mgorm.Connection{
			DriverName:      "godror",
			DataSourceName:  DSN,
			MaxIdleConns:    10,
			MaxOpenConns:    50,
			ConnMaxLifetime: 0,
			ConnMaxIdleTime: 0,
		}
	} else {

		DNS := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s", o.Duser, o.Dpassword, o.Dhost, o.Dport, o.Database,o.Charset)
		destConn = mgorm.Connection{
			DriverName:      "mysql",
			DataSourceName:  DNS,
			MaxIdleConns:    10,
			MaxOpenConns:    50,
			ConnMaxLifetime: 0,
			ConnMaxIdleTime: 0,}
	}
	a["source"] = &sourceConn
	a["dest"] = &destConn
	b.Database = o.Database
	b.Tablename = o.Tablename
	b.IgnoreTable = o.IgnoreTable
	b.ChunkSize,_ = strconv.Atoi(o.ChunkSize)
	return a
}
func GetCheckTableName(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) []string {
	var tableList []string
	//1、判断表明是否相同,列名是否相同，输出相同的表名
	MstrSql := fmt.Sprintf("show tables from %s;",o.Database)
	OstrSql := "SELECT table_NAME FROM USER_TABLES"
	SstrSql,DstrSql := PublicFunc.TypeSql(a,MstrSql,OstrSql)
	stableList := a["source"].SQLTableNum(SstrSql,o)
	dtableList := a["dest"].SQLTableNum(DstrSql,o)
	if len(stableList) !=0 && len(dtableList) !=0 {
		tableList = checksum.ColumnsValidation(stableList,dtableList)
	}else {
		fmt.Printf("[error]: The source-side database %s or the target-side database %s is empty without any tables\n", o.Database, o.Database)
		os.Exit(1)
	}

	if o.Tablename != "ALL" && len(o.Tablename) > 0{
		table := strings.Split(o.Tablename, ",")
		var ao []string
		for i := range table {
			for k := range tableList{
				if table[i] == tableList[k]{
					ao = append(ao,table[i])
				}
			}
		}
		var bo []string
		for i := range table{
			for k := range ao{
				if table[i] != ao[k]{
					bo = append(bo,table[i])
				}
			}
		}
		if len(bo) >0 {
			fmt.Println("The current output checklist does not exist at the source end. Please check the checklist：",bo)
            os.Exit(1)
		}
		tableList = table
	}
	// 是否忽略某个表的数据校验
	if o.IgnoreTable != "NULL" {
		var aa []string
		o.IgnoreTable = strings.ToUpper(o.IgnoreTable)
		a := strings.Split(o.IgnoreTable, ",")
		bmap := make(map[string]int)
		for _, v := range tableList {
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
		tableList = aa
	}
	o.TableList = tableList
	return tableList
}

func GetCheckColumnType(c []string,a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
	var tableList []string
	//1、判断表明是否相同,列名是否相同，输出相同的表名
	var sourColumnList,destColumnList []byte
	for _,ta := range c {
		MstrSql := fmt.Sprintf("SELECT column_name,data_type,numeric_scale from information_schema.columns where table_schema='%s' and table_name='%s' order by ORDINAL_POSITION;", o.Database, ta)
		OstrSql := fmt.Sprintf("SELECT column_name,data_type,data_scale FROM all_tab_cols WHERE table_name ='%s' order by column_id", ta)
		SstrSql,DstrSql := PublicFunc.TypeSql(a,MstrSql,OstrSql)
		o.Tablename = ta
		aa, _ := a["source"].SQLColumnsNum(SstrSql, o)
		sourColumnList = append(sourColumnList,aa...)
		sourColumnList = append(sourColumnList,";"...)
		bb, _ := a["dest"].SQLColumnsNum(DstrSql, o)
		destColumnList = append(destColumnList,bb...)
		destColumnList = append(destColumnList,";"...)
	}
	ac := checksum.ColumnsValidation(sourColumnList,destColumnList)
	for i:= range ac {
		aa := strings.Split(ac[i],":")
		tableList = append(tableList,aa[0])
	}
	o.TableList =  tableList
}

func GetCheckTablePRI(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
	MstrSql := fmt.Sprintf("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='%s' and table_name = '%s' and COLUMN_KEY='PRI' and COLUMN_TYPE like '%%int%%';",o.Database,o.Tablename)
	OstrSql := fmt.Sprintf("select a.column_name from user_cons_columns a, user_constraints b where a.constraint_name = b.constraint_name and b.constraint_type = 'P' and a.table_name = '%s'",o.Tablename)
	SstrSql,_ := PublicFunc.TypeSql(a,MstrSql,OstrSql)
	PRI := a["source"].SQLTablePRIColumn(SstrSql)
	if len(PRI) <1 {
		fmt.Printf("The current table %s does not have a primary key index. Please create a primary key index or use -igt to ignore the table validation\n",o.Tablename)
		os.Exit(1)
	}
	o.ColumnPRI = PRI
}
func GetCheckTableRows(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
	MstrSql := fmt.Sprintf("SELECT /* SQL_NO_CACHE */ count(1) FROM `%s`.`%s` FORCE INDEX(PRIMARY);",o.Database,o.Tablename)
	OstrSql := fmt.Sprintf("SELECT /* SQL_NO_CACHE */ count(1) FROM %s",o.Tablename)
	SstrSql,_ := PublicFunc.TypeSql(a,MstrSql,OstrSql)
	RowsCount := a["source"].SQLTableRows(SstrSql,o)
	TableRowsCount,_ := strconv.Atoi(RowsCount)
	if TableRowsCount <1 {
		fmt.Printf("The current source table %s is empty. Use the -igt parameter to ignore data validation for this table",o.Tablename)
		os.Exit(1)
	}
	o.TableRows = TableRowsCount
}
func GetTableFirstIndexVal(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
	//返回源表主键索引第一个索引值，以源表信息为主
	MstrSql := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` FORCE INDEX(PRIMARY) WHERE %s IS NOT NULL ORDER BY %s LIMIT 1;" ,o.ColumnPRI,o.Database,o.Tablename,o.ColumnPRI,o.ColumnPRI)
	OstrSql := fmt.Sprintf("select %s from %s where %s is not null and rownum<=1  order by %s",o.ColumnPRI,o.Tablename,o.ColumnPRI,o.ColumnPRI)
	SstrSql,_ := PublicFunc.TypeSql(a,MstrSql,OstrSql)
	firstIndexVal := a["source"].SQLTableStartVal(SstrSql,o)
	o.TableFirstIndexVal = firstIndexVal
}

func ComputerJobTask(ea map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
	// 统计总共有多少个job任务
	var indexQue []byte
	var JobNums int
	FirstIndexVal := o.TableFirstIndexVal
	rowCount := o.TableRows
	ColumnPRI := o.ColumnPRI
	indexQue = append(indexQue,FirstIndexVal...)
	//计算task任务数量

	JobNums = rowCount / o.ChunkSize
	if rowCount % o.ChunkSize != 0{
		JobNums = JobNums + 1
	}
	//生成每个任务的始末值
	if  rowCount > o.ChunkSize {
		firstIndex,_ := strconv.Atoi(FirstIndexVal)
		for i:=1;i< JobNums;i++ {
			endrowCown := o.ChunkSize
			if o.ChunkSize+firstIndex > rowCount {
				o.ChunkSize = rowCount - firstIndex
			}
			MstrSql := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` FORCE INDEX(`PRIMARY`) WHERE ((`%s` >= %d )) ORDER BY %s LIMIT %d, %d;",ColumnPRI,o.Database,o.Tablename,ColumnPRI,firstIndex,ColumnPRI,o.ChunkSize,2)
			OstrSql := fmt.Sprintf("SELECT %s FROM (SELECT %s FROM (SELECT %s FROM %s WHERE %s >= %d and ROWNUM >=1 and ROWNUM <=%d+2 ORDER BY %s ) ORDER BY %s DESC) WHERE ROWNUM <3 order by %s",ColumnPRI,ColumnPRI,ColumnPRI,o.Tablename,ColumnPRI,firstIndex,endrowCown,ColumnPRI,ColumnPRI,ColumnPRI)
			SstrSql,_ := PublicFunc.TypeSql(ea,MstrSql,OstrSql)
			d := ea["source"].SQLTablePoint(SstrSql,o)
				if len(d) >1 {
					firstIndex, _ = strconv.Atoi(d[1])
					indexQue = append(indexQue, "-"...)
					indexQue = append(indexQue, []byte(d[0])...)
					indexQue = append(indexQue, ","...)
					indexQue = append(indexQue, []byte(d[1])...)
				}else {
					indexQue = append(indexQue,"-"...)
					indexQue = append(indexQue,[]byte(d[0])...)
				}
			}
		}else {  //假如数据量小于chunksize的值时，直接生成始末段
			indexQue = append(indexQue,"-"...)
			indexQue = append(indexQue,strconv.Itoa(o.ChunkSize)...)
		}
	o.TableIndexQue = indexQue
}

func GetSelectColumnDispose (DriverName string,columnSclice []string) string{
	var SelectColumns string
	if DriverName == "mysql" {
		for i := 0; i < len(columnSclice)-1; i++ {
			d := strings.Split(columnSclice[i], ":")
			e := d[0]
			SelectColumns = SelectColumns + e
			SelectColumns = SelectColumns + ","
		}
	}
	if DriverName == "godror" {
		for i := 0; i < len(columnSclice)-1; i++ {
			d := strings.Split(columnSclice[i], ":")
			if strings.ToUpper(d[1]) == "DATE" {
				f := "to_char(" + d[0] + ",'yyyy-mm-dd hh24:mi:ss') as " +d[0]
				SelectColumns = SelectColumns + f
			} else if x, _ := strconv.Atoi(d[2]); x > 0 && x < 9999999999 {
				var xx []byte
				for i := 1; i <= x; i++ {
					xx = append(xx, '0')
				}
				e := "to_char(" + d[0] + ",'fm9999990." + string(xx) +"') as " +d[0]
				SelectColumns = SelectColumns + e
			} else {
				e := d[0]
				SelectColumns = SelectColumns + e
			}
			SelectColumns = SelectColumns + ","
		}
	}
	if strings.HasSuffix(SelectColumns, ",") {
		SelectColumns = SelectColumns[:len(SelectColumns)-1]
	}
	return SelectColumns
}
func GetSelectColumn(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo) {
		MstrSql := fmt.Sprintf("SELECT column_name,data_type,numeric_scale from information_schema.columns where table_schema='%s' and table_name='%s' order by ORDINAL_POSITION;", o.Database, o.Tablename)
		OstrSql := fmt.Sprintf("SELECT column_name,data_type,data_scale FROM all_tab_cols WHERE table_name ='%s' order by column_id", o.Tablename)
	    SstrSql, DstrSql := PublicFunc.TypeSql(a, MstrSql, OstrSql)
		_, aa := a["source"].SQLColumnsNum(SstrSql, o)
	    _, bb := a["dest"].SQLColumnsNum(DstrSql, o)
		c := strings.Split(string(aa), "@")
		d := strings.Split(string(bb), "@")
	    SSelectColumns := GetSelectColumnDispose(a["source"].DriverName,c)
	    DSelectColumns := GetSelectColumnDispose(a["dest"].DriverName,d)
	    if a["source"].DriverName == "mysql"{
			o.MySQLSelectColumn = SSelectColumns
		}else {
			o.OracleSelectColumn = SSelectColumns
		}
		if a["dest"].DriverName == "mysql"{
			o.MySQLSelectColumn = DSelectColumns
		}else {
			o.OracleSelectColumn = DSelectColumns
		}
}

func ExecCheckSumData(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo,p *flag.ConnParameter) {
	c := strings.Split(string(o.TableIndexQue),",")
	for _,aa := range c{
		bb := strings.Split(aa,"-")
		var first,end string
		if cap(bb) <= 1{
			first = bb[0]
			end = bb[0]
		}else{
			first = bb[0]
			end = bb[1]
		}
		OstrSql := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM %s WHERE %s  >= %s and %s <= %s" ,o.OracleSelectColumn,o.Tablename,o.ColumnPRI,first,o.ColumnPRI,end)
		MstrSql := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ %s FROM `%s`.`%s` FORCE INDEX(`PRIMARY`) WHERE %s >= %s and %s <= %s;" ,o.MySQLSelectColumn,o.Database,o.Tablename,o.ColumnPRI,first,o.ColumnPRI,end)
		SourceSelectSql,DestSelectSql := PublicFunc.TypeSql(a,MstrSql,OstrSql)
		_,sourceDateInfo := a["source"].SQLTableCheckSum(SourceSelectSql,o)
		_,destDateInfo := a["dest"].SQLTableCheckSum(DestSelectSql,o)
		checksum.ChunkValidation(a,o,p,sourceDateInfo,destDateInfo)
	}


}



func TableCheckActive(){
	var p flag.ConnParameter
	var b  mgorm.SummaryInfo
	a := ConnInitCheck(&p,&b)
	//获取到源目端相同的表，只针对当前这些表进行校验
	checksumTableList := GetCheckTableName(a,&b)
	//针对源目端相同的表进行表结构校验
	GetCheckColumnType(checksumTableList,a,&b)

	fmt.Printf(" -- database %s  initialization completes,begin initialization table -- \n", p.Database)
	for _,v := range b.TableList{
		start := time.Now()
		fmt.Printf(" -- Start initial database %s table %s -- \n", p.Database, v)
		fmt.Printf(" ** table %s  check start ** \n", v)

		sqlFile := "/tmp/"+ p.Database + "_" + v + ".sql"
		//sqlFile := "C:\\" + p.Database + "_" + v + ".sql"
		_, err := os.Stat(sqlFile)
		if err == nil {
			os.Remove(sqlFile)
		}

		b.Tablename = v
		GetCheckTablePRI(a,&b)
		GetCheckTableRows(a,&b)
		GetTableFirstIndexVal(a,&b)
		ComputerJobTask(a,&b)
		GetSelectColumn(a,&b)
		ExecCheckSumData(a,&b,&p)

		end := time.Now()
		curr := end.Sub(start)
		fmt.Printf(" ** table %s check completed ** \n", v)
		fmt.Printf(" ** check table %s time is %s ** \n", v, curr)
		fmt.Println()
	}
}
