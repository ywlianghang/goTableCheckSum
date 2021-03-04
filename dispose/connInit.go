package dispose

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"
	"goProject/flag"
	"time"
)

func DSNconn(o *flag.ConnParameter) {
	var sdbconn,ddbconn *sql.DB
	var err error

	if o.FrameworkCode == "mm"{
		sourDSN := o.Suser +":" + o.Spassword + "@tcp("+o.Shost+":"+o.Sport+")/"+o.Database+"?charset="+o.Charset
		destDSN := o.Duser +":" + o.Dpassword +"@tcp("+o.Dhost+":"+o.Dport+")/"+o.Database+"?charset="+o.Charset
		sdbconn, err = sql.Open("mysql",sourDSN)
		if err != nil {
			fmt.Println("Failed to open source MySQL database. Error message:",err)
			return
		}
		ddbconn, err = sql.Open("mysql",destDSN)
		if err != nil {
			fmt.Println("Failed to open target MySQL database with error message:",err)
			return
		}
		err = sdbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to source MySQL database with error message:",err)
			return
		}
		err = ddbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to target MySQL database with error message:",err)
			return
		}
		o.Dbdirct["smysql"] = sdbconn
		o.Dbdirct["dmysql"] = ddbconn
	}
	if o.FrameworkCode == "om"{
		sourDSN := fmt.Sprintf("user=%s password=%s connectString=%s:%s/%s",o.Suser,o.Spassword,o.Shost,o.Sport,o.OracleSid)
		destDSN := o.Duser +":" + o.Dpassword +"@tcp("+o.Dhost+":"+o.Dport+")/"+o.Database+"?charset="+o.Charset
		sdbconn, err = sql.Open("godror",sourDSN)
		if err != nil {
			fmt.Println("Failed to open source Oracle database. Error message: ",err)
			return
		}
		ddbconn, err = sql.Open("mysql",destDSN)
		if err != nil {
			fmt.Println("Failed to open target MySQL database with error message: ",err)
			return
		}
		err = sdbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to source Oracle database with error message:",err)
			return
		}
		err = ddbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to target MySQL database with error message:",err)
			return
		}
		o.Dbdirct["soracle"] = sdbconn
		o.Dbdirct["dmysql"] = ddbconn
	}
	if o.FrameworkCode == "mo"{
		sourDSN	:= o.Suser +":" + o.Spassword + "@tcp("+o.Shost+":"+o.Sport+")/"+o.Database+"?charset="+o.Charset
		destDSN := fmt.Sprintf("user=%s password=%s connectString=%s:%s/%s",o.Duser,o.Dpassword,o.Dhost,o.Dport,o.OracleSid)
		sdbconn, err = sql.Open("mysql",sourDSN)
		if err != nil {
			fmt.Println("Failed to open source MySQL database. Error message: ",err)
			return
		}
		ddbconn, err = sql.Open("godror",destDSN)
		if err != nil {
			fmt.Println("Failed to open target Oracle database with error message: ",err)
			return
		}
		err = sdbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to source MySQL database with error message:",err)
			return
		}
		err = ddbconn.Ping()
		if err != nil {
			fmt.Println("Failed to connect to target Oracle database with error message:",err)
			return
		}
		o.Dbdirct["smysql"] = sdbconn
		o.Dbdirct["doracle"] = ddbconn
	}

	sdbconn.SetMaxOpenConns(1000)
	sdbconn.SetMaxIdleConns(500)
	sdbconn.SetConnMaxLifetime(30*time.Second)

	ddbconn.SetMaxOpenConns(1000)
	ddbconn.SetMaxIdleConns(500)
	ddbconn.SetConnMaxLifetime(30*time.Second)
}

