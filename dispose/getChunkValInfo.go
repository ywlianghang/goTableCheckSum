package dispose

import (
	"database/sql"
	"fmt"
	"goProject/checksum"
	"log"
	"strconv"
	"strings"
)

type TableInfo struct {
	SourceDB *sql.DB
	DestDB  *sql.DB
	DBname,Tablename,PRIcolumn,FirstIndexPoint,EndIndexPoint,DataFix,CheckSum,WhereSql,SdatabaseType,DdatabaseType string
}
type ChunkInfo struct {
	JobNums,Count,ChunkSize,ChunkBef,ChunkEnd   int
	ChunkSizeStauts   bool
	ChunkIndexQue []byte
}

func EmtyTableCheck(databaseType string,DB *sql.DB,dbname string,tablename string) (int,bool){
	var rowCount int
	var status bool = true
	var strSql string
	var err error
	var stmt *sql.Stmt
	if databaseType == "oracle"{
		strSql = fmt.Sprintf("SELECT /* SQL_NO_CACHE */ count(1) FROM %s",tablename)
		stmt,err = DB.Prepare(strSql)
		err = stmt.QueryRow().Scan(&rowCount)
	}
	if databaseType == "mysql"{
		strSql = "SELECT /* SQL_NO_CACHE */ count(?) FROM `" + dbname + "`.`" + tablename +"` FORCE INDEX(PRIMARY);"
		stmt,err = DB.Prepare(strSql)
		err = stmt.QueryRow(1).Scan(&rowCount)
	}
	if err != nil{
		fmt.Printf("[error]: Failed to query total %s rows for table under current databases %s.The error message is : %s.\n",tablename,dbname,err)
		status = false
	}
	return rowCount,status
}

func ChunkValue(table *TableInfo,chunk *ChunkInfo) bool {  //查询源表的当前数据行总数，以源表的信息为准
	var status bool = true
	var cstrSql,strSql string
	var stmt *sql.Stmt
	var err error
	// 获取当前表的总行数
	if table.SdatabaseType == "oracle"{
		cstrSql = "SELECT /* SQL_NO_CACHE */ count(:1) FROM " + table.Tablename
		stmt,err = table.SourceDB.Prepare(cstrSql)
		err = stmt.QueryRow(table.PRIcolumn).Scan(&chunk.Count)
		strSql = fmt.Sprintf("select %s from %s where %s is not null and rownum<=1  order by %s",table.PRIcolumn,table.Tablename,table.PRIcolumn,table.PRIcolumn)
	}
	if table.SdatabaseType == "mysql"{
		cstrSql = "SELECT /* SQL_NO_CACHE */ count(?) FROM `" + table.DBname + "`.`" + table.Tablename +"` FORCE INDEX(PRIMARY);"
		stmt,err = table.SourceDB.Prepare(cstrSql)
		err = stmt.QueryRow(table.PRIcolumn).Scan(&chunk.Count)
		strSql = "SELECT /*!40001 SQL_NO_CACHE */ "+ table.PRIcolumn + " FROM `" + table.DBname + "`.`"+ table.Tablename +"` FORCE INDEX(PRIMARY) WHERE " + table.PRIcolumn+"  IS NOT NULL ORDER BY " + table.PRIcolumn +" LIMIT 1;"
	}
	if err != nil{
		fmt.Printf("[error]: Failed to query total %s rows for table under current databases %s.The error message is : %s.\n",table.Tablename,table.DBname,err)
		status = false
	}
	// 统计总共有多少个job任务
	chunk.JobNums = chunk.Count / chunk.ChunkSize
	if chunk.Count % chunk.ChunkSize != 0{
		chunk.JobNums = chunk.JobNums + 1
	}
	//返回源表主键索引第一个索引值，以源表信息为主
	stmt,err = table.SourceDB.Prepare(strSql)
	err = stmt.QueryRow().Scan(&table.FirstIndexPoint)
	if err != nil {
		fmt.Printf("[error]: Failed to query the table %s in the current databases %s for primary key information. Please check whether the table has data or primary key information of type int.\n",table.Tablename,table.DBname)
		status = false
	}
	return status
}

func ChunkPoint(table *TableInfo,chunk *ChunkInfo) bool{  //对所有chunk的开头、结尾索引节点进行处理，生成字节数组，返回数组 //目前只支持主键为int类型的
	var status bool = true
	var indexQue []byte
	var strSql string
	var err error
	var rows *sql.Rows
	var stmt *sql.Stmt
	indexQue = append(indexQue,table.FirstIndexPoint...)
	if chunk.Count > chunk.ChunkSize {
		for i:=1;i< chunk.JobNums;i++ {
			a, _ := strconv.Atoi(table.FirstIndexPoint)
			if chunk.ChunkSize+a > chunk.Count {
				chunk.ChunkSize = chunk.Count - a
			}
			if table.SdatabaseType == "oracle"{
				strSql = "SELECT /*!40001 SQL_NO_CACHE */ " + table.PRIcolumn + " FROM (SELECT ROWNUM r," + table.PRIcolumn + " FROM " + table.Tablename +" where id >= :1 and rownum >=1 and rownum <=:2+1 order by " + table.PRIcolumn + ") s  where r >= :3"
				stmt,err = table.SourceDB.Prepare(strSql)
				rows, err = stmt.Query(a, chunk.ChunkSize,chunk.ChunkSize)
			}
			if table.SdatabaseType == "mysql"{
				strSql = "SELECT /*!40001 SQL_NO_CACHE */ " + table.PRIcolumn + " FROM `" + table.DBname + "`." + table.Tablename + " FORCE INDEX(`PRIMARY`) WHERE ((`" + table.PRIcolumn + "` >= ?)) ORDER BY " + table.PRIcolumn + " LIMIT ?, ?;"
				stmt,err = table.SourceDB.Prepare(strSql)
				rows, err = stmt.Query(a, chunk.ChunkSize, 2)
			}

			if err != nil {
				fmt.Printf("[error]: Failed to query the Chunk beginning and end nodes of the table %s under the current database %s.The error message is: %s.\n",  table.Tablename,table.DBname,err)
				status = false
			}

			var c []string
			for rows.Next() {
				var b string
				rows.Scan(&b)
				c = append(c,b)
				table.FirstIndexPoint = b
			}
			if len(c) >1{
				indexQue = append(indexQue,"-"...)
				indexQue = append(indexQue,[]byte(c[0])...)
				indexQue = append(indexQue,","...)
				indexQue = append(indexQue,[]byte(c[1])...)
			}else {
				indexQue = append(indexQue,"-"...)
				indexQue = append(indexQue,[]byte(c[0])...)
				indexQue = append(indexQue,","...)
				indexQue = append(indexQue,[]byte(c[0])...)
			}
		}

	}else {
		indexQue = append(indexQue,"-"...)
		indexQue = append(indexQue,strconv.Itoa(chunk.ChunkSize)...)
	}
	chunk.ChunkIndexQue = indexQue
	return status
}

func ChunkPutNum(chunk *ChunkInfo){   //创建channel，并将每隔channel的job开头索引和结尾索引以字符串形式写入到管道中
	a := chunk.ChunkIndexQue
	b := strings.Split(string(a),",")
	stringChan := make(chan string,len(b))

	for i := range b{
		stringChan <- b[i]
	}
	close(stringChan)
}

func aa(stmt *sql.Stmt, err error,befIndex int, endIndex int) ([]string, []byte){
	var result  []byte
	rows, err := stmt.Query(befIndex,endIndex)
	if err != nil {
		log.Fatal("[error]: Failed to query date from database. Please check current database status！", err)
	}
	// 获取列名
	columns,_ := rows.Columns()
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	scanArgs := make([]interface{},len(values))
	for i := range values{
		//把sql.RawBytes类型的地址存进来
		scanArgs[i] = &values[i]
	}
	for rows.Next(){
		rows.Scan(scanArgs...)
		for _,col := range values {
			result = append(result,col...)
			result = append(result,"@"...)
		}
		result = append(result,","...)
	}
	defer rows.Close()
	return columns,result
}

func OracDateTypeDispose(databaseType string,DB *sql.DB,DBname,Tablename string) string{
	var f string
	_, b,columnsOk := ColumnsNum(databaseType,DB, DBname, Tablename)
	if !columnsOk {
		fmt.Println("aaa")
	}
	c := strings.Split(string(b), "@")
	if databaseType == "oracle" {
		for i := 0; i < len(c)-1; i++ {
			d := strings.Split(c[i], ":")
			if strings.ToUpper(d[1]) == "DATE" {
				e := "to_char(" + d[0] + ",'yyyy-mm-dd hh24:mi:ss')"
				f = f + e
			} else if x,_:= strconv.Atoi(d[2]);x >0 && x<9999999999 {
					var xx []byte
					for i :=1; i<=x;i++ {
						xx = append(xx,'0')
					}
					e := "to_char(" + d[0] + ",'fm9999990." + string(xx) +"')"
					f = f + e
			    } else {
				e := d[0]
				f = f + e
			}
			f = f + ","
		}
	}
	if databaseType == "mysql"{
		for i := 0; i < len(c)-1; i++ {
			d := strings.Split(c[i], ":")
			e := d[0]
			f = f + e
			f = f + ","
		}
	}
	if strings.HasSuffix(f, ",") {
		f = f[:len(f)-1]
	}
	return f
}

func SourceChunkQuery(table *TableInfo, befIndex int, endIndex int) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
	var columns []string
	var result []byte

	//源端数据源为orace
	if table.SdatabaseType == "oracle" {
		f := OracDateTypeDispose("oracle",table.SourceDB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f + " FROM " + table.Tablename + " WHERE " + table.PRIcolumn + " >= :1 and " + table.PRIcolumn + " <= :2"
		DB := table.SourceDB
		stmt, err := DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Source Oracle database. Please check current database status！", err)
		}
		columns, result = aa(stmt, err,befIndex, endIndex)
	}
    //源端数据源为mysql
	if table.SdatabaseType == "mysql" {
		f := OracDateTypeDispose("mysql",table.SourceDB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ " + f +" FROM `" + table.DBname + "`.`" + table.Tablename + "` FORCE INDEX(`PRIMARY`) WHERE " + table.PRIcolumn + ">= ? and " + table.PRIcolumn + " <=  ?;"
		DB := table.DestDB
		stmt, err := DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Source MySQL database. Please check current database status！", err)
		}
		columns, result = aa(stmt, err,befIndex, endIndex)
	}
	return columns,result
}
func DestChunkQuery(table *TableInfo, befIndex int, endIndex int) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
	var columns []string
	var result []byte
	//目标端数据源为MySQL
	if table.DdatabaseType == "oracle" {
		f := OracDateTypeDispose("oracle",table.DestDB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f + " FROM " + table.Tablename + " WHERE " + table.PRIcolumn + " >= :1 and " + table.PRIcolumn + " <= :2"
		DB := table.DestDB
		stmt,err := DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Dest Oracle database. Please check current database status！", err)
		}
		columns,result = aa(stmt,err,befIndex, endIndex)
	}

	//目标数据源为MySQL
	if table.DdatabaseType == "mysql" {
		f := OracDateTypeDispose("mysql",table.DestDB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f +" FROM `" + table.DBname + "`.`" + table.Tablename + "` FORCE INDEX(`PRIMARY`) WHERE " + table.PRIcolumn + ">= ? and " + table.PRIcolumn + " <=  ?;"
		DB := table.DestDB
		stmt,err := DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Dest MySQL database. Please check current database status！", err)
		}
		columns,result = aa(stmt,err,befIndex, endIndex)
	}

	return columns,result
}

func bb(stmt *sql.Stmt, err error)([]string, []byte){
	var result []byte
	rows, err := stmt.Query()
	columns,_ := rows.Columns()
	// 定义一个切片，长度是字段的个数，切片里面的元素类型是sql.RawBytes
	values := make([]sql.RawBytes,len(columns))
	//定义一个切片，元素类型是interface{}接口
	scanArgs := make([]interface{},len(values))
	for i := range values{
		//把sql.RawBytes类型的地址存进来
		scanArgs[i] = &values[i]
	}
	for rows.Next(){
		rows.Scan(scanArgs...)
		for _,col := range values {
			result = append(result,col...)
			result = append(result,"@"...)
		}
		result = append(result,","...)
	}
	defer rows.Close()
	return columns,result
}

func SwhereDataCheck(DB *sql.DB, table *TableInfo,Whereconditions string) ([]string, []byte){
	var columns []string
	var result []byte
	var stmt *sql.Stmt
	var err error

	//源端数据源为oracle
	if table.SdatabaseType == "oracle" {
		f := OracDateTypeDispose("oracle",DB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f + " FROM " + table.Tablename + " where "+ Whereconditions
		stmt,err = DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Source Oracle database. Please check current database status！", err)
		}
		columns,result = bb(stmt,err)
	}

	//源端数据源为MySQL
	if table.SdatabaseType == "mysql" {
		f := OracDateTypeDispose("mysql",DB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f +" FROM `" + table.DBname + "`.`" + table.Tablename + "` where "+ Whereconditions
		stmt,err = DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Source MySQL database. Please check current database status！", err)
		}
		columns,result = bb(stmt,err)
	}
	return columns,result
}


func DwhereDataCheck(DB *sql.DB, table *TableInfo,Whereconditions string) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
	var result []byte
	var columns []string
	var stmt *sql.Stmt
	var err error
	//目标端数据源为Oracle
	if table.DdatabaseType == "oracle" {
		f := OracDateTypeDispose("oracle",DB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f + " FROM " + table.Tablename + " where "+ Whereconditions

		stmt,err = DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Dest Oracle database. Please check current database status！", err)
		}
		columns,result = bb(stmt,err)
	}

	//目标端数据源为MySQL
	if table.DdatabaseType == "mysql" {
		f := OracDateTypeDispose("mysql",DB,table.DBname,table.Tablename)
		strSql := "SELECT /*!40001 SQL_NO_CACHE */ "+ f +" FROM `" + table.DBname + "`.`" + table.Tablename + "` where "+ Whereconditions
		stmt,err = DB.Prepare(strSql)
		if err != nil {
			log.Fatal("[error]: Failed to query date from Dest MySQL database. Please check current database status！", err)
		}
		columns,result = bb(stmt,err)
	}
	return columns,result
}


func ChanExecSql(table *TableInfo,chunk *ChunkInfo) { //针对每一个块的数据进行校验
	a := chunk.ChunkIndexQue
	b := strings.Split(string(a), ",")
	for i := range b {
		var bef, end int
		c := strings.Split(b[i], "-")
		if len(c) > 1 {
			bef, _ = strconv.Atoi(c[0])
			end, _ = strconv.Atoi(c[1])
		} else {
			bef, _ = strconv.Atoi(c[0])
			end, _ = strconv.Atoi(c[0])
		}
		_, sresult := SourceChunkQuery(table, bef, end)
		columns, dresult := DestChunkQuery(table, bef, end)
		checksum.ChunkValidation(table.DestDB, table.DBname, table.Tablename, columns, sresult, dresult, table.DataFix, table.CheckSum)
	}
}

func WhereExecSql(table *TableInfo){
	_,sresult := SwhereDataCheck(table.SourceDB,table,table.WhereSql)
	columns,dresult := DwhereDataCheck(table.DestDB,table,table.WhereSql)
	checksum.ChunkValidation(table.DestDB,table.DBname,table.Tablename,columns,sresult,dresult,table.DataFix,table.CheckSum)
}