package Incremental

import (
	"context"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"os"
)

// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
// flavor is mysql or mariadb
func QueryBinlogEvent() {

	cfg := replication.BinlogSyncerConfig{
		ServerID:  1613306,
		Flavor:   "mysql",
		Host:     "172.16.50.161",
		Port:     3306,
		User:     "pcms",
		Password: "pcms@123",
	}
	syncer := replication.NewBinlogSyncer(cfg)
	// Start sync with specified binlog file and position
	streamer, _ := syncer.StartSync(mysql.Position{"mysql-bin.000002", 234})
	//gtidSet := "5a701ab0-61fa-11eb-bf11-080027193a00:1-2,\n6e54398c-5a10-11eb-b356-080027193a00:1-45"
	// or you can start a gtid replication like
	//streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	for {
		ev, _ := streamer.GetEvent(context.Background())
		// Dump event
		ev.Dump(os.Stdout)
	}
	// or we can use a timeout context
	//for {
	//	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	//	ev, err := streamer.GetEvent(ctx)
	//	cancel()
	//	if err == context.DeadlineExceeded {
	//		// meet timeout
	//		continue
	//	}
	//	ev.Dump(os.Stdout)
	//}
}