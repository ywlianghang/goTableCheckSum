package dispose

import (
	"database/sql"
	"fmt"
	"goProject/checksum"
	"strings"
)

//获取目标端表列的信息，生成列名，用于验证源目端表明及列明是否一致
func ColumnsNum(databaseType string,DB *sql.DB,database string,table string) ([]byte,[]byte,bool){  //获取每个表的列信息
	var columnsList []byte
	var columnsInfo []byte
	var status bool = true
	var strSql string

	if databaseType == "oracle" {
		strSql = "SELECT column_name,data_type,data_scale FROM all_tab_cols WHERE table_name = '" + table +"'" + "order by column_id"
	}
	if databaseType == "mysql"{
		strSql = "SELECT column_name,data_type,numeric_scale from information_schema.columns where table_schema='" + database + "' and table_name= '" + table +"' order by ORDINAL_POSITION;"
	}
	stmt,err := DB.Prepare(strSql)
	rows,err := stmt.Query()
	if err != nil {
		fmt.Printf("Failed to get column information for the current table %s under the databases %s !The information is as follows:%s\n",table,database,err)
		status = false
	}
	for rows.Next(){
		var columns string
		var colDataType string
		var numericScale string
		rows.Scan(&columns,&colDataType,&numericScale)
		if len(numericScale) == 0  {
			numericScale = "9999999999"
		}
		columnsList = append(columnsList,columns...)
		columnsList = append(columnsList,"@"...)
		columnsInfo = append(columnsInfo,columns...)
		columnsInfo = append(columnsInfo,":"...)
		columnsInfo = append(columnsInfo,colDataType...)
		columnsInfo = append(columnsInfo,":"...)
		columnsInfo = append(columnsInfo,numericScale...)
		columnsInfo = append(columnsInfo,"@"...)
	}
	defer rows.Close()
	return columnsList,columnsInfo,status
}

func TableNum(databaseType string,DB *sql.DB,database string) ([]byte,bool){  //获取库下表和列的信息
	var tableList []byte
	var status bool = true
	var strSql string
	if databaseType == "oracle" {
		strSql = "SELECT table_NAME FROM USER_TABLES"
	}
	if databaseType == "mysql" {
		strSql = "show tables from " + database
	}
	stmt,err := DB.Prepare(strSql)
	rows,err := stmt.Query()
	if err != nil{
		fmt.Println("获取数据库%s的表信息失败！详细信息如下：%s",database,err)
		status = false
	}
	for rows.Next(){
		var tablename string
		rows.Scan(&tablename)
		tablename = strings.ToUpper(tablename)
		columns,_,columnsOk := ColumnsNum(databaseType,DB,database,tablename)
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
func QueryTablePRIColumn(databaseType string,db *sql.DB,dbname string,tablename string) (string,bool){  //初始化数据库，获取当前库下每个表是否有int类型主键
	// 获取当前主键信息
	var strSql string
	var status bool = true
	var PRIcolumn string
	if databaseType == "oracle"{
		strSql = fmt.Sprintf("select a.column_name from user_cons_columns a, user_constraints b where a.constraint_name = b.constraint_name and b.constraint_type = 'P' and a.table_name = '%s'",tablename)
	}
	if databaseType == "mysql"{
		strSql = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='" + dbname + "' and table_name = '" + tablename +"' and COLUMN_KEY='PRI' and COLUMN_TYPE like '%int%';"
	}
	stmt,err := db.Prepare(strSql)
	err = stmt.QueryRow().Scan(&PRIcolumn)
	if err != nil {
		status = false
	}
	return PRIcolumn,status
}

func DatabaseInitCheck(sdatabaseType,ddatabaseType string,sdb,ddb *sql.DB,database string) ([]string){  //初始化库数据，当前库下有哪些表，这些表是否相同，表结构是否相同，
	var tableList []string
	stableList,sstatus := TableNum(sdatabaseType,sdb,database)
	dtableList,dstatus := TableNum(ddatabaseType,ddb,database)
	if !sstatus {
		return tableList
	}
	if !dstatus {
		return tableList
	}
	if len(stableList) !=0 && len(dtableList) !=0 {
		tableList = checksum.ColumnsValidation(stableList,dtableList)
	}
	return tableList
}
