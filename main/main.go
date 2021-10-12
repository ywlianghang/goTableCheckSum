package main

import (
	"goProject/dispose"
)

func main(){
	dispose.TableCheckActive()

	//SELECT @rowid:=@rowid+1 as rowid,a FROM pcms.aa, (SELECT @rowid:=0) as init;
	//Incremental.QueryBinlogEvent()

}

