package mgorm

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"os"
	"strings"
	"time"
)

type Connection struct {
	DriverName, DataSourceName string
	MaxIdleConns,MaxOpenConns int
	ConnMaxLifetime,ConnMaxIdleTime time.Duration
}

type SummaryInfo struct{
	Database,Tablename,StrSql string
}

type DB interface {
	GetConnection() *sql.DB
}

type SqlExec interface {
	DB
	SQLColumnsNum(o *SummaryInfo) ([]byte,[]byte)
	SQLTableNum(o *SummaryInfo) ([]byte,bool)
	SQLTablePRIColumn(o *SummaryInfo) (string,bool)
}



func (con *Connection) GetConnection() *sql.DB {
	db,err:=sql.Open(con.DriverName,con.DataSourceName)
	if err != nil {
		fmt.Errorf("Failed to open to %s database with",con.DataSourceName)
		return nil
	}
	db.SetMaxIdleConns(con.MaxIdleConns)
	db.SetMaxOpenConns(con.MaxOpenConns)
	db.SetConnMaxLifetime(con.ConnMaxLifetime)
	db.SetConnMaxIdleTime(con.ConnMaxIdleTime)
	return db
}

func (con *Connection) SQLColumnsNum(o *SummaryInfo) ([]byte,[]byte){ //获取每个表的列信息
	var columnsList []byte
	var columnsInfo []byte
	dbconn:=con.GetConnection()
	defer dbconn.Close()

	stmt,err := dbconn.Prepare(o.StrSql)
	rows,err := stmt.Query()
	if err != nil {
		fmt.Printf("Failed to get column information for the current table %s under the databases %s !The information is as follows:%s\n",err)
		os.Exit(1)
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
	return columnsList,columnsInfo
}

func (m *Connection) SQLTableNum(o *SummaryInfo) ([]byte,bool) { //获取库下表和列的信息
	var tableList []byte
	var status bool = true

	dbconn := m.GetConnection()
	defer dbconn.Close()
	//	strSql := "show tables from " + o.Database
	stmt, err := dbconn.Prepare(o.StrSql)
	rows, err := stmt.Query()
	if err != nil {
		fmt.Println("获取数据库%s的表信息失败！详细信息如下：%s", o.Database, err)
		status = false
	}
	for rows.Next() {
		var tablename string
		rows.Scan(&tablename)
		tablename = strings.ToUpper(tablename)
		o.Tablename = tablename
		columns, _ := m.SQLColumnsNum(o)

		tableList = append(tableList, tablename...)
		tableList = append(tableList, ":"...)
		tableList = append(tableList, columns...)
		tableList = append(tableList, ";"...)
	}
	return tableList, status
}

func (m *Connection) QueryMySQLTablePRIColumn(o *SummaryInfo) (string,bool){ //初始化数据库，获取当前库下每个表是否有int类型主键
	// 获取当前主键信息
	var status bool = true
	var PRIcolumn string

	dbconn := m.GetConnection()
	defer dbconn.Close()

	//strSql := "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where table_schema='" + o.Database + "' and table_name = '" + o.Tablename +"' and COLUMN_KEY='PRI' and COLUMN_TYPE like '%int%';"
	stmt,err := dbconn.Prepare(o.StrSql)
	err = stmt.QueryRow().Scan(&PRIcolumn)
	if err != nil {
		status = false
	}
	return PRIcolumn,status
}