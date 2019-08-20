package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/abourget/viperbind"
	pd "github.com/emicklei/protobuf2map"
	_ "github.com/eoscanada/pbop/pb/dfuse/codecs/deos"
	_ "github.com/eoscanada/pbop/pb/dfuse/codecs/deth"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{Use: "pbop", Short: "Inspects any file with most auto-detection and auto-discovery", RunE: root}
var peekCmd = &cobra.Command{Use: "peek", Short: "Do all sorts of type checks to determine what the file is", RunE: peek}
var decodeCmd = &cobra.Command{Use: "decode", Short: "Decode a file in a certain way", RunE: decode}

func main() {
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "PBOP")
	})

	rootCmd.AddCommand(peekCmd)
	rootCmd.AddCommand(decodeCmd)

	rootCmd.PersistentFlags().StringSliceP("include", "I", []string{}, "Path to protofiles")
	rootCmd.PersistentFlags().StringP("proto", "p", "", "Proto file")
	rootCmd.PersistentFlags().StringP("type", "t", "", "A (partial) type. Will crawl the .proto files in -I and do fnmatch")
	rootCmd.PersistentFlags().StringP("input", "i", "-", "Input file. '-' for stdin (default)")

	peekCmd.Flags().String("blockmeta-addr", "blockmeta:50001", "Blockmeta endpoint is queried to find the last irreversible block on the network being indexed")

	decodeCmd.Flags().Bool("enable-upload", false, "Upload merged indexes to the --indexes-store")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println("ERROR:", err)
		os.Exit(1)
	}
}

func root(cmd *cobra.Command, args []string) (err error) {
	// TODO: find the .proto file corresponding to an fnmatch of `-I`
	defs := pd.NewDefinitions()

	includes := viper.GetStringSlice("global-include")
	if len(includes) == 0 {
		return fmt.Errorf("add at least one --include (-I)")
	}

	var importedFiles []string
	err = filepath.Walk(includes[0], func(path string, info os.FileInfo, err error) error {
		shortPath := strings.TrimPrefix(strings.TrimPrefix(path, includes[0]), "/")

		//fmt.Println("PATH", info.IsDir(), path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			if strings.HasPrefix(shortPath, ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(info.Name(), ".proto") {
			cnt, err := ioutil.ReadFile(path)
			if err != nil {
				return fmt.Errorf("reading file %q: %s", path, err)
			}

			if err := defs.ReadFrom(shortPath, bytes.NewReader(cnt)); err != nil {
				return fmt.Errorf("parsing .proto file %q: %s", path, err)
			}

			fmt.Println("Imported", shortPath)

			importedFiles = append(importedFiles, shortPath)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walking: %s", err)
	}

	searchType := viper.GetString("global-type")

	var typesFound []string
	var packageFound, typeFound string
first:
	for _, importedFile := range importedFiles {
		//fmt.Println("FILE", importedFile)
		pkg, ok := defs.Package(importedFile)
		if !ok {
			continue
		}

		//fmt.Println("PKG", pkg)
		for _, msg := range defs.MessagesInPackage(pkg) {
			def := fmt.Sprintf("%s.%s", pkg, msg.Name)
			//fmt.Println("MESSAGE", def)
			if def == searchType {
				packageFound = pkg
				typeFound = msg.Name
				typesFound = []string{"ok"}
				break first
			}
			if strings.Contains(def, searchType) {
				typesFound = append(typesFound, def)
				packageFound = pkg
				typeFound = msg.Name
			}
		}
	}
	if len(typesFound) > 1 {
		return fmt.Errorf("ambiguous type (-t) provided, be more specific: %s", typesFound)
	}

	input := viper.GetString("global-input")
	var reader io.ReadCloser
	if input == "-" {
		reader = os.Stdin
	} else {
		file, err := os.Open(input)
		if err != nil {
			return err
		}
		defer file.Close()

		reader = file
	}

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, reader)
	if err != nil {
		return
	}

	dec := pd.NewDecoder(defs, proto.NewBuffer(buf.Bytes()))

	result, err := dec.Decode(packageFound, typeFound)
	if err != nil {
		return fmt.Errorf("decoding: %s", err)
	}

	log.Printf("%#v", result)

	return nil
}
func peek(cmd *cobra.Command, args []string) (err error) {
	fmt.Println("PEEK")
	return nil
}
func decode(cmd *cobra.Command, args []string) (err error) {
	fmt.Println("DECODE")
	return nil
}

func resolveProtoFile(input string) (file string, err error) {
	return "", nil
}
