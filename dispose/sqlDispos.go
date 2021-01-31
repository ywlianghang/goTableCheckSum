package dispose

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"goProject/GoTableCheckSum/checksum"
	"log"
	"strconv"
	"strings"
)

type TableInfo struct {
	SourceDB *sql.DB
	DestDB  *sql.DB
	DBname string
	Tablename string
	PRIcolumn string
	FirstIndexPoint  string
	EndIndexPoint string
	DataFix string
	CheckSum string
	WhereSql string
}
type ChunkInfo struct {
	JobNums   int
	Count   int
	ChunkSize   int
	ChunkSizeStauts   bool
	ChunkBef   int
	ChunkEnd   int
	ChunkIndexQue []byte
}

func EmtyTableCheck(DB *sql.DB,dbname string,tablename string) (int,bool){
	var rowCount int
	var status bool = true
	strSql := "SELECT /* SQL_NO_CACHE */ count(?) FROM `" + dbname + "`.`" + tablename +"` FORCE INDEX(PRIMARY);"
	err := DB.QueryRow(strSql,1).Scan(&rowCount)
	if err != nil{
		fmt.Printf("[error]: Failed to query total %s rows for table under current databases %s.The error message is : %s.\n",tablename,dbname,err)
		status = false
	}
	return rowCount,status
}

func ChunkValue(table *TableInfo,chunk *ChunkInfo) bool {  //查询源表的当前数据行总数，以源表的信息为准
	var status bool = true
	// 获取当前表的总行数
	strSql := "SELECT /* SQL_NO_CACHE */ count(?) FROM `" + table.DBname + "`.`" + table.Tablename +"` FORCE INDEX(PRIMARY);"
	err := table.SourceDB.QueryRow(strSql,table.PRIcolumn).Scan(&chunk.Count)
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
	strSql = "SELECT /*!40001 SQL_NO_CACHE */ "+ table.PRIcolumn + " FROM `" + table.DBname + "`.`"+ table.Tablename +"` FORCE INDEX(PRIMARY) WHERE " + table.PRIcolumn+"  IS NOT NULL ORDER BY " + table.PRIcolumn +" LIMIT 1;"
	err = table.SourceDB.QueryRow(strSql).Scan(&table.FirstIndexPoint)
	if err != nil {
		fmt.Printf("[error]: Failed to query the table %s in the current databases %s for primary key information. Please check whether the table has data or primary key information of type int.\n",table.Tablename,table.DBname)
		status = false
	}
	return status
}

func ChunkPoint(table *TableInfo,chunk *ChunkInfo) bool{  //对所有chunk的开头、结尾索引节点进行处理，生成字节数组，返回数组 //目前只支持主键为int类型的
   var status bool = true
   var indexQue []byte
   indexQue = append(indexQue,table.FirstIndexPoint...)
   for i:=1;i< chunk.JobNums;i++ {
	   a, _ := strconv.Atoi(table.FirstIndexPoint)
	   if chunk.ChunkSize+a > chunk.Count {
		   chunk.ChunkSize = chunk.Count - a
	   }
	   strSql := "SELECT /*!40001 SQL_NO_CACHE */ " + table.PRIcolumn + " FROM `" + table.DBname + "`." + table.Tablename + " FORCE INDEX(`PRIMARY`) WHERE ((`" + table.PRIcolumn + "` >= ?)) ORDER BY " + table.PRIcolumn + " LIMIT ?, ?;"
	   rows, err := table.SourceDB.Query(strSql, a, chunk.ChunkSize, 2)
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

func ChunkQuery(DB *sql.DB, table *TableInfo, befIndex int, endIndex int) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
	var result  []byte
	strSql := "SELECT /*!40001 SQL_NO_CACHE */ * FROM `" + table.DBname +"`.`" + table.Tablename +"` FORCE INDEX(`PRIMARY`) WHERE "+ table.PRIcolumn + ">= ? and "+ table.PRIcolumn +" <=  ?;"
	rows, err := DB.Query(strSql,befIndex,endIndex)
   // 获取列名
	if err != nil {
		log.Fatal("[error]: Failed to query date from database. Please check current database status！", err)
	}
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

func WhereDataCheck(DB *sql.DB, table *TableInfo,Whereconditions string) ([]string, []byte) { //根据每个chunk的起始节点，生成查询语句，去数据库查询数据，返回数据的字节数组
	var result  []byte
	strSql := "SELECT /*!40001 SQL_NO_CACHE */ * FROM `" + table.DBname +"`.`" + table.Tablename + "` where "+ Whereconditions +";"
	rows, err := DB.Query(strSql)
	// 获取列名
	if err != nil {
		log.Fatal("[error]: Failed to query date from database. Please check current database status！", err)
	}
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
		columns, sresult := ChunkQuery(table.SourceDB, table, bef, end)
		_, dresult := ChunkQuery(table.DestDB, table, bef, end)
		checksum.ChunkValidation(table.DestDB, table.DBname, table.Tablename, columns, sresult, dresult, table.DataFix, table.CheckSum)
	}
}
func WhereExecSql(table *TableInfo){
		columns,sresult := WhereDataCheck(table.SourceDB,table,table.WhereSql)
		_,dresult := WhereDataCheck(table.DestDB,table,table.WhereSql)
		checksum.ChunkValidation(table.DestDB,table.DBname,table.Tablename,columns,sresult,dresult,table.DataFix,table.CheckSum)

}






//type QueryJobStruct struct {
//	QueryChunk chan int
//	n int
//}
//
//
//func PrintIndex(a,b int){
//	fmt.Println(a,b)
//}
//func ChunkGetNum(DB *sql.DB,table *TableInfo,stringChan <- chan string){
//	gonum :=10
//	for {
//		//fmt.Println("-1-1-",len(stringChan))
//		if len(stringChan) >0 {
//			var bef int
//			var end int
//			for i := 1; i <= gonum; i++ {
//				a,ok := <- stringChan
//				bb := strings.Split(a,"-")
//				if len(bb) >1 {
//					bef,_ = strconv.Atoi(bb[0])
//					end,_ = strconv.Atoi(bb[1])
//				}else {
//					bef,_ = strconv.Atoi(bb[0])
//					end,_ = strconv.Atoi(bb[0])
//				}
//				if !ok {
//					break
//				}
//				go PrintIndex(bef,end)
//				//go ChunkQuery(DB,table,bef,end)
//			}
//		}else{
//			break
//		}
//	}
//}


func QueryChunk(DB *sql.DB,table *TableInfo,chunk *ChunkInfo) {

	//gonum := 10
	//writejobChan := make(chan QuerySqlJobStruct, gonum)
	//writech := make(chan int,gonum)
	//for i:=1;i<=gonum;i++{
	//	writejob := QuerySqlJobStruct{
	//		writech:writech,
	//	}
	//	writejobChan <- writejob
	//}
}
