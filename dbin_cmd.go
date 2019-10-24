package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/eoscanada/dbin" // internal model, until we switch it all to Protobuf
	pbbstream "github.com/eoscanada/doh/pb/dfuse/bstream/v1"
	pbdeos "github.com/eoscanada/doh/pb/dfuse/codecs/deos"
	pbdeth "github.com/eoscanada/doh/pb/dfuse/codecs/deth"
	"github.com/eoscanada/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tidwall/sjson"
)

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

	depth := viper.GetInt("dbin-cmd-depth")
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
		case pbbstream.Protocol_EOS:
			out, err = decodeInDepth(out, marshaler, depth-1, &pbdeos.Block{}, el.PayloadBuffer, "payload_buffer")
		case pbbstream.Protocol_ETH:
			out, err = decodeInDepth(out, marshaler, depth-1, &pbdeth.Block{}, el.PayloadBuffer, "payload_buffer")
		default:
			return "", fmt.Errorf("unsupported protocol: %s", el.PayloadKind)
		}
		if err != nil {
			return
		}
	case *pbdeos.Block:
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
