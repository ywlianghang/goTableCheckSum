package checksum

import (
	"bufio"
	"database/sql"
	"fmt"
	"goProject/PublicFunc"
	mgorm "goProject/mgorm/ExecQuerySQL"
	"os"
	"strings"
)

type DesTableInfo struct {
	dbname  string
	tablename string
	DestConn  *sql.DB
}

func DestInsert(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo,insertData []string) []string{
	var MySQLinsertVal string
	var MySQLinsertValByte,OracleDateColumnName []byte
	var DestinsertValstring []string

	tmpOracleSelectColumnStringSlice := strings.Split(o.OracleSelectColumn,",")
	for k,v := range tmpOracleSelectColumnStringSlice {
		if strings.Index(v, "'yyyy-mm-dd hh24:mi:ss')") != -1 {
			tmpIndex := k - 1
			OracleDateColumnName = append(OracleDateColumnName,strings.Split(tmpOracleSelectColumnStringSlice[tmpIndex],"(")[1]...)
			OracleDateColumnName = append(OracleDateColumnName,","...)
		}
	}

	for _,v := range insertData {
		var MySQLinsertDataByte,OracleinsertDataByte []byte
		var MySQLsingleInsertVal,OraclesingleInsertVal string
		aa := strings.Split(v,"&@")
		MySQLinsertDataByte = append(MySQLinsertDataByte,"("...)
		for _,k := range aa {
			b := strings.Split(k,"&:")
			if len(b) > 1 {
				if a["dest"].DriverName == "mysql" {
					MySQLinsertDataByte = append(MySQLinsertDataByte,"'"...)
					MySQLinsertDataByte = append(MySQLinsertDataByte,b[1]...)
					MySQLinsertDataByte = append(MySQLinsertDataByte,"'"...)
					MySQLinsertDataByte = append(MySQLinsertDataByte,","...)
				}
				if a["dest"].DriverName == "godror" {
					if  strings.Index(string(OracleDateColumnName),b[0]) != -1 {
						OracleinsertDataByte = append(OracleinsertDataByte, "to_date('"...)
						OracleinsertDataByte = append(OracleinsertDataByte, b[1]...)
						OracleinsertDataByte = append(OracleinsertDataByte,"','yyyy-mm-dd hh24:mi:ss')"...)
						OracleinsertDataByte = append(OracleinsertDataByte,","...)
					}else {
						OracleinsertDataByte = append(OracleinsertDataByte,"'"...)
						OracleinsertDataByte = append(OracleinsertDataByte, b[1]...)
						OracleinsertDataByte = append(OracleinsertDataByte,"'"...)
						OracleinsertDataByte = append(OracleinsertDataByte,","...)
					}
				}
			}
		}
		if strings.HasSuffix(string(OracleinsertDataByte),",") && len(string(OracleinsertDataByte)) >1{
			OraclesingleInsertVal = string(OracleinsertDataByte)
			OraclesingleInsertVal = OraclesingleInsertVal[:len(OraclesingleInsertVal)-1]
			OinsertSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", o.Tablename,o.MySQLSelectColumn,OraclesingleInsertVal)
			DestinsertValstring = append(DestinsertValstring,OinsertSql)
		}
		if strings.HasSuffix(string(MySQLinsertDataByte),",") {
			MySQLsingleInsertVal = string(MySQLinsertDataByte)
			MySQLsingleInsertVal = MySQLsingleInsertVal[:len(MySQLsingleInsertVal)-1]
			MySQLinsertValByte = append(MySQLinsertValByte,MySQLsingleInsertVal...)
			MySQLinsertValByte = append(MySQLinsertValByte,"),"...)
		}
	}
	if  strings.HasSuffix(string(MySQLinsertValByte),",") && len(string(MySQLinsertValByte)) >1{
		MySQLinsertVal = string(MySQLinsertValByte)
		MySQLinsertVal = MySQLinsertVal[:len(MySQLinsertVal)-1]
		MinsertSql := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES %s;", o.Database,o.Tablename,o.MySQLSelectColumn,MySQLinsertVal)
		DestinsertValstring = append(DestinsertValstring,MinsertSql)
	}
	return DestinsertValstring
}

func DestDelete(a map[string]*mgorm.Connection,o *mgorm.SummaryInfo,deleteData []string) string{  //生成delete语句，删除目标端多余的数据，数据有差异也是先删除，后插入
	var deleteSql string
	var deleteValByte []byte
	for _,v := range deleteData {
		aa := strings.Split(v,"&@")
		for _,k := range aa {
			b := strings.Split(k,"&:")
			if b[0] == strings.ToUpper(o.ColumnPRI) {
				deleteValByte = append(deleteValByte,b[1]...)
				deleteValByte = append(deleteValByte,","...)
			}
		}
	}
	deleteVal := string(deleteValByte)
	if strings.HasSuffix(deleteVal,","){
		deleteVal = deleteVal[:len(deleteVal)-1]
	}
	MdeleteSql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s IN (%s);",o.Database,o.Tablename,o.ColumnPRI,deleteVal)
	OdeleteSql := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",o.Tablename,o.ColumnPRI,deleteVal)
	_,deleteSql = PublicFunc.TypeSql(a,MdeleteSql,OdeleteSql)
    return deleteSql

}
func SqlFile(dbname string,tablename string,sql string){  //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
	sqlFile := "/tmp/"+ dbname + "_" + tablename + ".sql"
	//sqlFile := "C:\\"+ dbname + "_" + tablename + ".sql"
	sfile,err := os.Open(sqlFile)
	if err != nil && os.IsNotExist(err){
		sfile,err = os.OpenFile(sqlFile,os.O_WRONLY|os.O_CREATE,0666)
	}else {
		sfile,err = os.OpenFile(sqlFile,os.O_WRONLY|os.O_APPEND,0666)
	}

	if err != nil {
		fmt.Printf("open file err=%v\n",err)
	}
	write := bufio.NewWriter(sfile)
	write.WriteString(sql + "\n")
	write.Flush()
	defer sfile.Close()

}