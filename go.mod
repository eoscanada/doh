module github.com/eoscanada/doh

go 1.12

require (
	cloud.google.com/go v0.51.0
	cloud.google.com/go/bigtable v1.2.0
	github.com/abourget/viperbind v0.1.0
	github.com/coreos/etcd v3.3.10+incompatible
	github.com/dustin/go-humanize v1.0.0
	github.com/eoscanada/bstream v1.7.1-0.20200326121139-a9fcd944fb9e
	github.com/eoscanada/dbin v0.0.3
	github.com/eoscanada/dstore v0.1.5
	github.com/eoscanada/jsonpb v0.0.0-20191003191457-98439e8ce04b
	github.com/eoscanada/kvdb v0.0.12-0.20200402161342-124fe3102056
	github.com/golang/protobuf v1.3.4
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/klauspost/compress v1.8.5
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.3.2
	github.com/tcnksm/go-gitconfig v0.1.2
	github.com/tidwall/sjson v1.0.4
	google.golang.org/api v0.15.0
	gopkg.in/src-d/go-git.v4 v4.13.1
)

replace github.com/eoscanada/kvdb => /home/abourget/dfuse/kvdb
