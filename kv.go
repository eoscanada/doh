package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/eoscanada/kvdb"
	"github.com/eoscanada/kvdb/store"
	_ "github.com/eoscanada/kvdb/store/badger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var kvCmd = &cobra.Command{Use: "kv", Short: "Read from a KVStore"}
var kvPrefixCmd = &cobra.Command{Use: "prefix", Short: "prefix read from KVStore", RunE: kvPrefix, Args: cobra.ExactArgs(1)}
var kvScanCmd = &cobra.Command{Use: "scan", Short: "scan read from KVStore", RunE: kvScan, Args: cobra.ExactArgs(2)}
var kvGetCmd = &cobra.Command{Use: "get", Short: "get key from KVStore", RunE: kvGet, Args: cobra.ExactArgs(1)}

func init() {
	rootCmd.AddCommand(kvCmd)

	kvCmd.AddCommand(kvPrefixCmd)
	kvCmd.AddCommand(kvScanCmd)
	kvCmd.AddCommand(kvGetCmd)

	kvCmd.PersistentFlags().StringP("store", "s", "badger:///dfusebox-data/kvdb/kvdb_badger.db", "KVStore DSN")
	kvCmd.PersistentFlags().IntP("depth", "d", 1, "Depth of decoding. 0 = top-level block, 1 = kind-specific blocks, 2 = future!")
	kvCmd.PersistentFlags().StringP("protocol", "p", "", "block protocol value to assume of the data")

	kvScanCmd.Flags().IntP("limit", "l", 100, "limit the number of rows when doing scan")
}

func kvPrefix(cmd *cobra.Command, args []string) (err error) {
	kv, err := store.New(viper.GetString("kv-cmd-store"))
	if err != nil {
		return err
	}

	prefix, err := hex.DecodeString(args[0])
	if err != nil {
		return fmt.Errorf("error decoding prefix %q: %s", args[0], err)
	}
	it := kv.Prefix(context.Background(), prefix)
	for it.Next() {
		item := it.Item()
		fmt.Println(hex.EncodeToString(item.Key), hex.EncodeToString(item.Value))
	}
	if err := it.Err(); err != nil {
		return err
	}

	return nil
}

func kvScan(cmd *cobra.Command, args []string) (err error) {
	kv, err := store.New(viper.GetString("kv-cmd-store"))
	if err != nil {
		return err
	}

	start, err := hex.DecodeString(args[0])
	if err != nil {
		return fmt.Errorf("error decoding range start %q: %s", args[0], err)
	}
	end, err := hex.DecodeString(args[1])
	if err != nil {
		return fmt.Errorf("error decoding range end %q: %s", args[1], err)
	}

	limit := viper.GetInt("kv-scan-cmd-limit")

	it := kv.Scan(context.Background(), start, end, limit)
	for it.Next() {
		item := it.Item()
		fmt.Println(hex.EncodeToString(item.Key), hex.EncodeToString(item.Value))
	}
	if err := it.Err(); err != nil {
		return err
	}

	return nil
}

func kvGet(cmd *cobra.Command, args []string) (err error) {
	kv, err := store.New(viper.GetString("kv-cmd-store"))
	if err != nil {
		return err
	}

	key, err := hex.DecodeString(args[0])
	if err != nil {
		return fmt.Errorf("error decoding range start %q: %s", args[0], err)
	}

	val, err := kv.Get(context.Background(), key)
	if err == kvdb.ErrNotFound {
		os.Exit(1)
	}

	fmt.Println(hex.EncodeToString(key), hex.EncodeToString(val))

	return nil
}
