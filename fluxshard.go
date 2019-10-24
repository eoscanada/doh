package main

import (
	"encoding/gob"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/eoscanada/doh/fluxdb"
	"github.com/eoscanada/dstore"
	"github.com/spf13/cobra"
)

func viewFluxShard(cmd *cobra.Command, args []string) (err error) {
	// Take the first param, use as filename, read as zstd
	baseFile := filepath.Base(args[0])
	storeURL := strings.TrimSuffix(args[0], baseFile)
	store, err := dstore.NewStore(storeURL, "shard.zst", "zstd", false)
	if err != nil {
		return err
	}

	read, err := store.OpenObject(strings.TrimSuffix(baseFile, ".shard.zst"))
	if err != nil {
		return err
	}
	defer read.Close()

	decoder := gob.NewDecoder(read)
	encoder := json.NewEncoder(os.Stdout)
	for {
		req := new(fluxdb.WriteRequest)
		err := decoder.Decode(&req)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if err := encoder.Encode(req); err != nil {
			return err
		}
	}
	return nil
}
