package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"cloud.google.com/go/bigtable"

	"github.com/abourget/viperbind"
	pbbstream "github.com/eoscanada/bstream/pb/dfuse/bstream/v1"
	"github.com/eoscanada/dbin"
	"github.com/eoscanada/pbop/jsonpb"
	pbdeos "github.com/eoscanada/pbop/pb/dfuse/codecs/deos"
	pbdeth "github.com/eoscanada/pbop/pb/dfuse/codecs/deth"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tidwall/sjson"
)

var rootCmd = &cobra.Command{Use: "pbop", Short: "Inspects any file with most auto-detection and auto-discovery", RunE: root}
var dbinCmd = &cobra.Command{Use: "dbin", Short: "Do all sorts of type checks to determine what the file is", RunE: viewDbin}
var btCmd = &cobra.Command{Use: "bt", Short: "big table related things"}
var btLsCmd = &cobra.Command{Use: "ls", Short: "list tables form big table", RunE: btLs}
var btReadCmd = &cobra.Command{Use: "read [table]", Short: "read rows from big table", RunE: btRead, Args: cobra.ExactArgs(1)}

var protoMappings = map[pbbstream.BlockKind]map[string]proto.Message{
	pbbstream.BlockKind_ETH: map[string]proto.Message{
		"block_headerProto": &pbdeth.Block{},
		"trx_proto":         &pbdeth.TransactionTrace{},
	},
}

func main() {
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "PBOP")
	})

	rootCmd.AddCommand(dbinCmd)
	rootCmd.AddCommand(btCmd)
	btCmd.AddCommand(btLsCmd)
	btCmd.AddCommand(btReadCmd)

	rootCmd.PersistentFlags().StringP("type", "t", "", "A (partial) type. Will crawl the .proto files in -I and do fnmatch")
	rootCmd.PersistentFlags().StringP("input", "i", "-", "Input file. '-' for stdin (default)")
	rootCmd.PersistentFlags().IntP("depth", "d", 1, "Depth of decoding. 0 = top-level block, 1 = kind-specific blocks, 2 = future!")
	btCmd.PersistentFlags().String("db", "test:dev", "bigtable project and instance")
	btReadCmd.Flags().String("prefix", "", "bigtable prefix key")
	btReadCmd.Flags().String("type", "", "chain type")
	//dbinCmd.Flags().BoolP("list", "l", false, "Return as list instead of as JSONL")
	//

	//decodeCmd.Flags().Bool("enable-upload", false, "Upload merged indexes to the --indexes-store")
	if err := rootCmd.Execute(); err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}
}

func root(cmd *cobra.Command, args []string) (err error) {
	searchType := viper.GetString("global-type")

	knownTypes := []string{
		"dfuse.bstream.v1.Block",
		"dfuse.codecs.deth.Block",
		"dfuse.codecs.deth.BlockHeader",
		"dfuse.codecs.deth.EVMCall",
		"dfuse.codecs.deos.SignedBlock",
	}
	var matchingType string
	for _, t := range knownTypes {
		if searchType == t {
			matchingType = t
			break
		}
		if strings.Contains(t, searchType) {
			if matchingType != "" {
				return fmt.Errorf("ambiguous type (-t) provided (%q or %q ?), be more specific (known types: %q)", matchingType, t, knownTypes)
			}
			matchingType = t
		}
	}
	if matchingType == "" {
		return fmt.Errorf("type (-t) doesn't match known types (%q)", knownTypes)
	}

	reader, err := inputFile(args)
	if err != nil {
		return err
	}
	defer reader.Close()

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, reader)
	if err != nil {
		return
	}

	var el proto.Message
	typ := proto.MessageType(matchingType)
	el = reflect.New(typ.Elem()).Interface().(proto.Message)

	depth := viper.GetInt("global-depth")
	pbmarsh := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		OrigName:     true,
	}

	out, err := decodeInDepth("", pbmarsh, depth, el, buf.Bytes(), "")
	if err != nil {
		return err
	}
	fmt.Println(out)

	return nil
}

func viewDbin(cmd *cobra.Command, args []string) (err error) {
	// Open the dbin file
	// Check its type
	// Load the contents with the right value

	reader, err := inputFile(args)
	if err != nil {
		return err
	}
	defer reader.Close()

	binReader := dbin.NewReader(reader)

	contentType, version, err := binReader.ReadHeader()
	if version != 1 {
		return fmt.Errorf("unsupported dbin version %d", version)
	}

	switch contentType {
	case "EOS", "ETH":
	default:
		return fmt.Errorf("unsupported dbin content type: %s", contentType)
	}

	depth := viper.GetInt("global-depth")
	pbmarsh := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		OrigName:     true,
	}

	for {
		msg, err := binReader.ReadMessage()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading message: %s", err)
		}

		out, err := decodeInDepth("", pbmarsh, depth, &pbbstream.Block{}, msg, "")
		if err != nil {
			return err
		}

		fmt.Println(out)
	}

	return nil
}

func btLs(cmd *cobra.Command, args []string) (err error) {
	project, instance, err := splitDb()
	if err != nil {
		return err
	}
	client, _ := newBigTableAdminClient(project, instance)
	tables, err := client.Tables(context.Background())
	if err != nil {
		return err
	}
	fmt.Println("Listing tables")
	for _, tbl := range tables {
		fmt.Println(tbl)
	}
	return nil
}

func btRead(cmd *cobra.Command, args []string) (err error) {
	project, instance, err := splitDb()
	if err != nil {
		return err
	}

	flagBlockKind := viper.GetString("bt-read-cmd-type")

	blockKind := pbbstream.BlockKind(pbbstream.BlockKind_value[flagBlockKind])
	if blockKind == pbbstream.BlockKind_UNKNOWN {
		return fmt.Errorf("invalid block kind value: %q", flagBlockKind)
	}

	prefix := viper.GetString("bt-read-cmd-prefix")
	client, err := newBigTableClient(project, instance)
	if err != nil {
		return err
	}

	pbmarsh := jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: true,
		OrigName:     true,
	}

	var innerError error

	err = client.Open(args[0]).ReadRows(context.Background(), bigtable.PrefixRange(prefix), func(row bigtable.Row) bool {
		formatedRow := map[string]interface{}{}
		for _, v := range row {
			for _, item := range v {
				key := strings.Replace(item.Column, "-", "_", -1)
				key = strings.Replace(key, ":", "_", -1)
				protoMessage := getProtoMap(blockKind, key)
				if protoMessage != nil {
					formatedRow[key], _ = decodePayload(pbmarsh, protoMessage, item.Value)
				} else {
					formatedRow[key] = item.Value
				}
			}
		}
		cnt, err := json.Marshal(formatedRow)
		if err != nil {
			innerError = err
			return false
		}
		fmt.Println(string(cnt))
		return true
	}, bigtable.LimitRows(5))
	if err != nil {
		return err
	}
	if innerError != nil {
		return innerError
	}

	return nil
}

func decodeInDepth(inputJSON string, marshaler jsonpb.Marshaler, depth int, obj proto.Message, bytes []byte, replaceField string) (out string, err error) {
	if depth < 0 {
		return inputJSON, nil
	}

	err = proto.Unmarshal(bytes, obj)
	if err != nil {
		return "", fmt.Errorf("proto unmarshal: %s", err)
	}

	out, err = marshaler.MarshalToString(obj)
	if err != nil {
		return "", fmt.Errorf("json marshal: %s", err)
	}

	switch el := obj.(type) {
	case *pbbstream.Block:
		switch el.PayloadKind {
		case pbbstream.BlockKind_EOS:
			// FIXME: &pbdeos.SignedBlock{} when we have reprocessed everything with BlockKind_EOS instead of an int
			out, err = decodeInDepth(out, marshaler, depth-1, &pbdeth.Block{}, el.PayloadBuffer, "payloadBuffer")
		case pbbstream.BlockKind_ETH:
			out, err = decodeInDepth(out, marshaler, depth-1, &pbdeth.Block{}, el.PayloadBuffer, "payloadBuffer")
		default:
			return "", fmt.Errorf("unsupported block kind: %s", el.PayloadKind)
		}
		if err != nil {
			return
		}
	case *pbdeos.SignedBlock:
	case *pbdeth.Block:
	}

	if inputJSON != "" {
		out, err = sjson.Set(inputJSON, replaceField, json.RawMessage(out))
		if err != nil {
			return out, fmt.Errorf("sjson: %s", err)
		}
	}

	return
}

func decode(cmd *cobra.Command, args []string) (err error) {
	fmt.Println("DECODE")
	return nil
}

func inputFile(args []string) (io.ReadCloser, error) {
	if len(args) > 0 {
		return os.Open(args[0])
	}

	input := viper.GetString("global-input")
	if input == "-" {
		return os.Stdin, nil
	}

	return os.Open(input)
}

func splitDb() (project, instance string, err error) {
	parts := strings.Split(viper.GetString("bt-global-db"), ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid --db field")
	}
	return parts[0], parts[1], nil
}

func getProtoMap(blockKindValue pbbstream.BlockKind, key string) proto.Message {
	maps := protoMappings[blockKindValue]
	if val, ok := maps[key]; ok {
		return val
	} else {
		return nil
	}

}

func decodePayload(marshaler jsonpb.Marshaler, obj proto.Message, bytes []byte) (out proto.Message, err error) {

	err = proto.Unmarshal(bytes, obj)
	if err != nil {
		return nil, fmt.Errorf("proto unmarshal: %s", err)
	}

	return obj, nil
}
