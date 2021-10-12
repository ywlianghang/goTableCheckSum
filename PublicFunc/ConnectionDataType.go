package PublicFunc

import mgorm "goProject/mgorm/ExecQuerySQL"

func TypeSql(a map[string]*mgorm.Connection, m,o string) (string,string) {
	var SstrSql,DstrSql string
	//1、判断表明是否相同,列名是否相同，输出相同的表名
	if a["source"].DriverName == "mysql" {
		SstrSql = m
	}else{
		SstrSql = o
	}
	if a["dest"].DriverName == "mysql" {
		DstrSql = m
	}else{
		DstrSql = o
	}
	return SstrSql,DstrSql
}
