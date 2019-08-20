module github.com/eoscanada/pbop

go 1.12

require (
	github.com/abourget/viperbind v0.1.0
	github.com/emicklei/proto v1.6.13 // indirect
	github.com/emicklei/protobuf2map v0.0.0-20181105121648-9f44bd8300be
	github.com/eoscanada/bstream v1.6.3-0.20190819195625-0ef8a3ad6a9b
	github.com/eoscanada/dbin v0.0.3
	github.com/gogo/protobuf v1.2.0
	github.com/golang/protobuf v1.3.2
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.3.2
	github.com/tidwall/sjson v1.0.4
	google.golang.org/genproto v0.0.0-20190716160619-c506a9f90610
	google.golang.org/grpc v1.23.0
)

replace github.com/emicklei/protobuf2map => /home/abourget/go/src/github.com/emicklei/protobuf2map
