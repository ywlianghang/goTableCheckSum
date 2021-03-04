package checksum

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

type DesTableInfo struct {
	dbname  string
	tablename string
	DestConn  *sql.DB
}

func DestInsert(dbname string,tablename string,columnInfo []string,strsql []string) []string{ //生成insert语句，将目标端缺失的sql语句插入
    var col []byte
    var sql []string
	col = append(col,"insert into `"...)
	col = append(col,dbname...)
	col = append(col,"`.`"...)
	col = append(col,tablename...)
	col = append(col,"` ("...)
    for k,l := range columnInfo{
    	col = append(col,l...)
    	if k < len(columnInfo)-1 {
			col = append(col, ","...)
		}
	}
	col = append(col,") "...)
    for _,v := range strsql{
		v = strings.Replace(v, "@", "','", -1)
		v = "'" + v[:strings.LastIndex(v,",'")]
		v = "values (" + v + ");"
		a := string(col) + v
		sql = append(sql,a)
	}
	return sql
}

func DestDelete(dbname string,tablename string,columnInfo []string,strsql []string) []string{  //生成delete语句，删除目标端多余的数据，数据有差异也是先删除，后插入
	var col []byte
	var sql []string
	delcol := columnInfo[0]
	col = append(col,"delete from `"...)
	col = append(col,dbname...)
	col = append(col,"`.`"...)
	col = append(col,tablename...)
	col = append(col,"` "...)
	col = append(col,"where "...)
	col = append(col,delcol...)
	col = append(col," = "...)
	for _,v := range strsql{
		del := strings.Split(v,"@")[0]
		a := string(col) + del + ";"
		sql = append(sql,a)
	}
   return sql

}
func SqlFile(dbname string,tablename string,sql []string){  //在/tmp/下创建数据修复文件，将在目标端数据修复的语句写入到文件中
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
	for i := range sql{
		write.WriteString(sql[i]+"\n")
	}
	write.Flush()
	defer sfile.Close()

}

func SqlExec(descconn *sql.DB,sql []string){   //执行目标端数据修复语句
	for i:= range sql{
		stm,_ := descconn.Prepare(sql[i])
		stm.Exec()
	}
}