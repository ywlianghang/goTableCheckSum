package dispose

import (
	"database/sql"
	"fmt"
	"goProject/GoTableCheckSum/checksum"
)

func ColumnsNum(DB *sql.DB,database string,table string) ([]byte,bool){  //获取每个表的列信息
	var columnsList []byte
	var status bool = true
	strSql := "select COLUMN_NAME from information_schema.columns where table_schema='" + database + "' and table_name= '" + table +"';"
	rows,err := DB.Query(strSql)
	if err != nil {


		fmt.Printf("Failed to get column information for the current table %s under the databases %s !The information is as follows:%s\n",table,database,err)
		status = false
	}
	for rows.Next(){
		var columns string
		rows.Scan(&columns)
		columnsList = append(columnsList,columns...)
		columnsList = append(columnsList,"@"...)
	}
	defer rows.Close()
	return columnsList,status
}

func TableNum(DB *sql.DB,database string) ([]byte,bool){  //获取库下表和列的信息
	var tableList []byte
	var status bool = true
	strSql := "show tables from " + database
	rows,err := DB.Query(strSql)
	if err != nil{
		fmt.Println("获取数据库%s的表信息失败！详细信息如下：%s",database,err)
		status = false
	}
	for rows.Next(){
		var tablename string
		rows.Scan(&tablename)
		columns,columnsOk := ColumnsNum(DB,database,tablename)
		if !columnsOk{
			status = false
			break
		}
		tableList = append(tableList,tablename...)
		tableList = append(tableList,":"...)
		tableList = append(tableList,columns...)
		tableList = append(tableList,";"...)
	}
	return tableList,status
}
func QueryTablePRIColumn(db *sql.DB,dbname string,tablename string) (string,bool){  //初始化数据库，获取当前库下每个表是否有int类型主键
	// 获取当前主键信息
	var status bool = true
	var PRIcolumn string
	strSql := "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='" + dbname + "' and table_name = '" + tablename +"' and COLUMN_KEY='PRI' and COLUMN_TYPE like '%int%';"
	err := db.QueryRow(strSql).Scan(&PRIcolumn)
	if err != nil {
		status = false
	}
	return PRIcolumn,status
}


func DatabaseInitCheck(source ,dest *sql.DB,database string) ([]string){  //初始化库数据，当前库下有哪些表，这些表是否相同，表结构是否相同，
	stableList,sstatus := TableNum(source,database)
	var tableList []string
	if !sstatus {
		return tableList
	}
	dtableList,dstatus := TableNum(dest,database)
	if !dstatus {
		return tableList
	}
	if len(stableList) !=0 && len(dtableList) !=0 {
		tableList = checksum.ColumnsValidation(stableList,dtableList)
	}
	return tableList
}

