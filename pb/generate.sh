#!/bin/bash

ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

current_dir="`pwd`"
trap "cd \"$current_dir\"" EXIT
pushd "$ROOT" &> /dev/null

# Service definitions
SERVICES=${1:-../../service-definitions}

# pushd $SERVICES
#   shopt -s globstar
#   FILES=$(ls **/*.proto)

#   for file in $FILES; do
#     # protoc -I. --go_out=plugins=grpc,paths=source_relative,Mdfuse/bstream/v1/bstream.proto=dfuse/bstream/v1:$ROOT $file
#     protoc -I. --go_out=plugins=grpc,paths=import:$ROOT $file
#   done

#   echo "package pbfiles

# var Files = \`$FILES\`
# " > $ROOT/files.go
# popd

protoc -I$SERVICES --go_out=paths=source_relative:. dfuse/codecs/deth/deth.proto
protoc -I$SERVICES --go_out=paths=source_relative:. dfuse/codecs/deos/deos.proto
