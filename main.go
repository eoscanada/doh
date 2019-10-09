package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path"
	"reflect"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/abourget/viperbind"
	"github.com/eoscanada/dbin"
	pbbstream "github.com/eoscanada/doh/pb/dfuse/bstream/v1"
	pbdeos "github.com/eoscanada/doh/pb/dfuse/codecs/deos"
	pbdeth "github.com/eoscanada/doh/pb/dfuse/codecs/deth"
	"github.com/eoscanada/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/tcnksm/go-gitconfig"
	"github.com/tidwall/sjson"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

var rootCmd = &cobra.Command{Use: "doh", Short: "Inspects any file with most auto-detection and auto-discovery", SilenceUsage: true}
var pbCmd = &cobra.Command{Use: "pb", Short: "Decode protobufs", RunE: pb}
var dbinCmd = &cobra.Command{Use: "dbin", Short: "Do all sorts of type checks to determine what the file is", RunE: viewDbin}
var btCmd = &cobra.Command{Use: "bt", Short: "big table related things"}
var btLsCmd = &cobra.Command{Use: "ls", Short: "list tables form big table", RunE: btLs}
var btReadCmd = &cobra.Command{Use: "read [table]", Short: "read rows from big table", RunE: btRead, Args: cobra.ExactArgs(1)}
var deployCmd = &cobra.Command{Use: "deploy [component] [tag] [namespace]", Short: "deploy the following `component` using `tag` on given `namespace`", RunE: deploy, Args: cobra.ExactArgs(3)}

var completionCmd = &cobra.Command{Use: "shell-completion", Short: "Generate shell completions"}
var completionBashCompletionCmd = &cobra.Command{Use: "bash", Short: "Generate bash completion file output",
	Run: func(cmd *cobra.Command, args []string) {
		if err := rootCmd.GenBashCompletion(os.Stdout); err != nil {
			log.Fatal(err)
		}
	},
}

var completionZshCompletionCmd = &cobra.Command{
	Use:   "zsh",
	Short: "Generate zsh completion file output",
	Run: func(cmd *cobra.Command, args []string) {
		if err := rootCmd.GenZshCompletion(os.Stdout); err != nil {
			log.Fatal(err)
		}
	},
}

var protoMappings = map[pbbstream.Protocol]map[string]proto.Message{
	pbbstream.Protocol_EOS: map[string]proto.Message{
		"block_proto":         &pbdeos.Block{},
		"trxs_trxRefsProto":   &pbdeos.TransactionRefs{},
		"trxs_traceRefsProto": &pbdeos.TransactionRefs{},
		"trx_proto":           &pbdeos.SignedTransaction{},
		"trace_proto":         &pbdeos.TransactionTrace{},
		"dtrx_created-by":     &pbdeos.ExtDTrxOp{},
		"dtrx_canceled-by":    &pbdeos.ExtDTrxOp{},
		"meta_blockheader":    &pbdeos.BlockHeader{},
	},

	pbbstream.Protocol_ETH: map[string]proto.Message{
		"block_headerProto":  &pbdeth.BlockHeader{},
		"block_trxRefsProto": &pbdeth.TransactionRefs{},
		"block_uncles":       &pbdeth.UnclesHeaders{},
		"trx_proto":          &pbdeth.TransactionTrace{},
		"trx_blkRefProto":    &pbdeth.BlockRef{},
	},
}

func main() {
	cobra.OnInitialize(func() {
		viperbind.AutoBind(rootCmd, "DOH")
	})

	rootCmd.AddCommand(dbinCmd)
	rootCmd.AddCommand(pbCmd)
	rootCmd.AddCommand(btCmd)
	rootCmd.AddCommand(deployCmd)
	rootCmd.AddCommand(completionCmd)

	btCmd.AddCommand(btLsCmd)
	btCmd.AddCommand(btReadCmd)

	completionCmd.AddCommand(completionZshCompletionCmd)
	completionCmd.AddCommand(completionBashCompletionCmd)

	pbCmd.Flags().StringP("type", "t", "", "A (partial) type. Will crawl the .proto files in -I and do fnmatch")
	pbCmd.Flags().StringP("input", "i", "-", "Input file. '-' for stdin (default)")
	pbCmd.Flags().IntP("depth", "d", 1, "Depth of decoding. 0 = top-level block, 1 = kind-specific blocks, 2 = future!")
	dbinCmd.Flags().IntP("depth", "d", 1, "Depth of decoding. 0 = top-level block, 1 = kind-specific blocks, 2 = future!")
	btCmd.PersistentFlags().String("db", "dfuseio-global:dfuse-saas", "bigtable project and instance")
	btReadCmd.Flags().String("prefix", "p", "bigtable prefix key")
	btReadCmd.Flags().String("protocol", "", "block protocol value to assume of the data")
	btReadCmd.Flags().IntP("limit", "l", 100, "limit the number of rows returned")
	btReadCmd.Flags().IntP("depth", "d", 1, "Depth of decoding. 0 = top-level block, 1 = kind-specific blocks, 2 = future!")
	//dbinCmd.Flags().BoolP("list", "l", false, "Return as list instead of as JSONL")
	//decodeCmd.Flags().Bool("enable-upload", false, "Upload merged indexes to the --indexes-store")
	deployCmd.Flags().String("operator-path", "", "Absolute path to dfuse-operator repository")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func pb(cmd *cobra.Command, args []string) (err error) {
	searchType := viper.GetString("pb-cmd-type")

	var matchingType string
	for _, t := range knownProtobufTypes {
		if searchType == t {
			matchingType = t
			break
		}
		if strings.Contains(t, searchType) {
			if matchingType != "" {
				return fmt.Errorf("ambiguous type (-t) provided (%q or %q ?), be more specific (known types: %q)", matchingType, t, knownProtobufTypes)
			}
			matchingType = t
		}
	}

	if matchingType == "" {
		return fmt.Errorf("type (-t) doesn't match known types (%q)", knownProtobufTypes)
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

	depth := viper.GetInt("pb-cmd-depth")
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

func btLs(cmd *cobra.Command, args []string) (err error) {
	project, instance, err := splitDb()
	if err != nil {
		return err
	}
	client, err := newBigTableAdminClient(project, instance)
	if err != nil {
		return fmt.Errorf("init bigtable admin client: %s", err)
	}

	tables, err := client.Tables(context.Background())
	if err != nil {
		return fmt.Errorf("listing tables: %s", err)
	}

	fmt.Println("Listing tables:")
	for _, tbl := range tables {
		fmt.Println("-", tbl)
	}
	return nil
}

var latestCellOnly = bigtable.LatestNFilter(1)
var latestCellFilter = bigtable.RowFilter(latestCellOnly)

func btRead(cmd *cobra.Command, args []string) (err error) {
	project, instance, err := splitDb()
	if err != nil {
		return err
	}

	flagProtocol := viper.GetString("bt-read-cmd-protocol")
	depth := viper.GetInt("bt-read-cmd-depth")
	protocol := pbbstream.Protocol(pbbstream.Protocol_value[flagProtocol])
	if protocol == pbbstream.Protocol_UNKNOWN {
		return fmt.Errorf("invalid block --protocol value: %q", flagProtocol)
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

	opts := []bigtable.ReadOption{
		latestCellFilter,
	}
	if limit := viper.GetInt("bt-read-cmd-limit"); limit != 0 {
		opts = append(opts, bigtable.LimitRows(int64(limit)))
	}

	err = client.Open(args[0]).ReadRows(context.Background(), bigtable.PrefixRange(prefix), func(row bigtable.Row) bool {
		formatedRow := map[string]interface{}{
			"_key": row.Key(),
		}

		for _, v := range row {
			for _, item := range v {
				key := strings.Replace(item.Column, "-", "_", -1)
				key = strings.Replace(key, ":", "_", -1)
				protoMessage := getProtoMap(protocol, key)
				if (protoMessage != nil) && (depth != 0) {
					formatedRow[key], err = decodePayload(pbmarsh, protoMessage, item.Value)
					if err != nil {
						innerError = err
						return false
					}
				} else {
					formatedRow[key] = string(item.Value)
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
	}, opts...)

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

func decode(cmd *cobra.Command, args []string) (err error) {
	fmt.Println("DECODE")
	return nil
}

func inputFile(args []string) (io.ReadCloser, error) {
	if len(args) > 0 {
		return os.Open(args[0])
	}

	input := viper.GetString("pb-cmd-input")
	if input == "-" {
		return os.Stdin, nil
	}

	return os.Open(input)
}

func splitDb() (project, instance string, err error) {
	db := viper.GetString("bt-global-db")
	if db == "prod" {
		return "dfuseio-global", "dfuse-saas", nil
	}

	parts := strings.Split(db, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid --db field")
	}
	return parts[0], parts[1], nil
}

func getProtoMap(protocolValue pbbstream.Protocol, key string) proto.Message {
	maps := protoMappings[protocolValue]
	if val, ok := maps[key]; ok {
		typ := reflect.TypeOf(val)
		return reflect.New(typ.Elem()).Interface().(proto.Message)
	}

	return nil
}

func decodePayload(marshaler jsonpb.Marshaler, obj proto.Message, bytes []byte) (out json.RawMessage, err error) {
	err = proto.Unmarshal(bytes, obj)
	if err != nil {
		return nil, fmt.Errorf("proto unmarshal: %s", err)
	}

	cnt, err := marshaler.MarshalToString(obj)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %s", err)
	}

	return json.RawMessage(cnt), nil
}

func deploy(cmd *cobra.Command, args []string) error {
	component := args[0]
	tag := args[1]
	namespace := args[2]

	// To finalize:
	//  - Support variable number of arguments for `namespace`
	//  - Support argument to be only `eos` or `eth` (deploy on all `eos` network) (No ondemand by default? Behing a flag?)
	//  - Support `all` argument? (No ondemand by default? Behing a flag?)

	operatorPath := viper.GetString("deploy-cmd-operator-path")
	if operatorPath == "" {
		return errors.New("the dfuse operator path repository must be specified")
	}

	info, err := os.Stat(operatorPath)
	if os.IsNotExist(err) {
		errorLines := []string{
			"dfuse-operator repository '%s' path does not exist, we suggest you to globally",
			"set the environment variable DOH_DEPLOY_CMD_OPERATOR_PATH and making it point",
			"to your 'dfuse-operator' repository path.",
		}

		return fmt.Errorf(strings.Join(errorLines, "\n"), operatorPath)
	}

	if err != nil {
		return fmt.Errorf("unable to get info about operator path '%s': %s", operatorPath, err)
	}

	if !info.IsDir() {
		return fmt.Errorf("operator path '%s' is not a directory", operatorPath)
	}

	fmt.Printf("Deploying '%s:%s' on namespace '%s' ...\n", component, tag, namespace)

	repo, err := git.PlainOpen(operatorPath)
	if err != nil {
		return fmt.Errorf("unable to open operator repository: %s", err)
	}

	workTree, err := repo.Worktree()
	if err != nil {
		return fmt.Errorf("unable to obtain work tree: %s", err)
	}

	repoConfig, err := repo.Config()
	if err != nil {
		return fmt.Errorf("unable to retrieve repository config: %s", err)
	}

	username, err := gitconfig.Username()
	if err != nil {
		return fmt.Errorf("unable to retrieve 'user.name' config option: %s", err)
	}

	email, err := gitconfig.Email()
	if err != nil {
		return fmt.Errorf("unable to retrieve 'user.email' config option: %s", err)
	}

	if repoConfig.Remotes["origin"] == nil {
		return errors.New("only able to deploy if dfuse operator repository has an 'origin' remote, none found")
	}

	isDirtyRepo, err := isDirty(workTree)
	if err != nil {
		return fmt.Errorf("unable to determine if operator repository is dirty: %s", err)
	}

	if isDirtyRepo {
		return errors.New("refusing to deploy since operator repository is dirty, please fix that prior deploying")
	}

	fmt.Println("Updating images file with new container image...")

	namespaceRelativePath := path.Join("hooks", fmt.Sprintf("%s.jsonnet", namespace))
	namespaceFile := path.Join(operatorPath, namespaceRelativePath)
	_, err = os.Stat(namespaceFile)
	if os.IsNotExist(err) {
		return fmt.Errorf("namespace file '%s' could not be found, unable to deploy on this namespace", namespaceRelativePath)
	}

	parts := strings.SplitN(namespace, "-", 2)
	protocol := parts[0]

	// FIXME: Need to handle versionning somehow, hard-coded for testing purposes
	imagesRelativePath := path.Join("hooks", "components", protocol, "v1", "_images.libsonnet")
	imagesFile := path.Join(operatorPath, imagesRelativePath)
	imagesContent, err := ioutil.ReadFile(imagesFile)
	if err != nil {
		return fmt.Errorf("unable to read images file '%s' to update component tag: %s", imagesRelativePath, err)
	}

	// FIXME: What's the right syntax to re-use arg?
	componentImageRegex := regexp.MustCompile(fmt.Sprintf("%s: 'gcr.io/eoscanada-shared-services/%s:.+'", component, component))
	matches := componentImageRegex.Match(imagesContent)
	if !matches {
		return fmt.Errorf("unable to find component image line in images file '%s'", imagesRelativePath)
	}

	// FIXME: What's the right syntax to re-use arg?
	componentImageLine := fmt.Sprintf("%s: 'gcr.io/eoscanada-shared-services/%s:%s'", component, component, tag)
	modifiedImagesContent := componentImageRegex.ReplaceAllString(string(imagesContent), componentImageLine)

	err = ioutil.WriteFile(imagesFile, []byte(modifiedImagesContent), os.ModePerm)
	if err != nil {
		return fmt.Errorf("unable to write updated component image tag: %s", err)
	}

	// Let's reset the work tree at this point whatever happens. We discard the error also, too bad
	// if we were not able to clean up correctly.
	defer (func() {
		workTree.Reset(&git.ResetOptions{
			Mode: git.HardReset,
		})
	})()

	isDirtyRepo, err = isDirty(workTree)
	if err != nil {
		return fmt.Errorf("unable to determine if operator repository is dirty: %s", err)
	}

	if isDirtyRepo {
		fmt.Println("Refreshing last run files, this takes ~1m ...")
		err = refreshLastRunFiles(operatorPath)
		if err != nil {
			return fmt.Errorf("unable to refresh last run files (via 'test.sh' script): %s", err)
		}

		fmt.Println("Comitting changes ...")
		_, err = workTree.Commit(fmt.Sprintf("[doh] updated %s to image id %s for namespace %s", component, tag, namespace), &git.CommitOptions{
			// By using `All`, our modified files will be automatically added into the commit
			All: true,
			Author: &object.Signature{
				Name:  "doh (Automated Deploy)",
				Email: "doh@dfuse.io",
				When:  time.Now(),
			},
			Committer: &object.Signature{
				Name:  username,
				Email: email,
				When:  time.Now(),
			},
		})

		if err != nil {
			return fmt.Errorf("unable to commit operator changes: %s", err)
		}
	}

	fmt.Println("Pushing changes to remote 'origin' ...")
	err = repo.Push(&git.PushOptions{
		RemoteName: "origin",
	})

	if err != nil && err != git.NoErrAlreadyUpToDate {
		return fmt.Errorf("unable to push changes to remote repository: %s", err)
	}

	head, err := repo.Head()
	if err != nil {
		return fmt.Errorf("unable to retrieve HEAD revision hash: %s", err)
	}

	fmt.Println("Patching k8s dfuse Cluster resource ...")
	updateK8s := exec.Command("kubectl",
		"-n", namespace,
		"patch", "dfuseclusters.dfuse.io", namespace,
		"--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"revision":"%s"}}`, head.Hash().String()),
	)

	err = updateK8s.Run()
	if err != nil {
		return fmt.Errorf("unable to update 'dfuseclusters.dfuse.io' resource %s: %s", namespace, err)
	}

	fmt.Println("Successfully deployed.")
	return nil
}

func refreshLastRunFiles(operatorPath string) error {
	testFilePath := path.Join(operatorPath, "test.sh")

	cmd := exec.Command(testFilePath)
	cmd.Dir = operatorPath

	return cmd.Run()
}

func isDirty(workTree *git.Worktree) (bool, error) {
	status, err := workTree.Status()
	if err != nil {
		return false, err
	}

	return !status.IsClean(), nil
}
