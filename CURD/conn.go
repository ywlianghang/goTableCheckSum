package CURD

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"time"
)


func DSNconn(a map[string]string) (*sql.DB,*sql.DB,bool){
	var status  bool = true
	sourDSN := a["suser"] +":" + a["spassword"] + "@tcp("+a["shost"]+":"+a["sport"]+")/"+a["database"]+"?charset="+a["charset"]

	sdbconn, err := sql.Open("mysql",sourDSN)
	if err != nil {
		//log.Fatal("连接mysql错误，错误信息如下：", err)
		status = false
	}
	destDSN := a["duser"] +":" + a["dpassword"] +"@tcp("+a["dhost"]+":"+a["dport"]+")/"+a["database"]+"?charset="+a["charset"]

	ddbconn, err := sql.Open("mysql",destDSN)
	if err != nil {
		//log.Fatal("连接mysql错误，错误信息如下：", err)
		status = false
	}
	err = sdbconn.Ping()
	if err != nil {
		//fmt.Println("源数据库无法连接",err)
		status = false
	}
	err = ddbconn.Ping()
	if err != nil {
		//fmt.Println("目标数据库无法连接",err)
		status = false
	}
	sdbconn.SetMaxOpenConns(1000)
	sdbconn.SetMaxIdleConns(500)
	sdbconn.SetConnMaxLifetime(30*time.Second)

	ddbconn.SetMaxOpenConns(1000)
	ddbconn.SetMaxIdleConns(500)
	ddbconn.SetConnMaxLifetime(30*time.Second)
	return sdbconn,ddbconn,status
}

