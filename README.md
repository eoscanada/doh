pbop - pry open those protobuf files
------------------------------------

pbop is aware of our block structures, of dbin packing and can render most of the things to JSON
with decent unpacking.

Install with:

    go get -u github.com/eoscanada/pbop


Usage:

```bash
$ pbop -t bstream.v1.Block -i ../search/testdata/eth/02-block-with-logs.dat -d 1 | jq . | less
{...}

$ pbop dbin -d 0 oneblock.eth.dbin | jq . | less
{...}
{...}
```

The `-d` flag represents the depth of decoding.. when decoding known
structures, we can go deeper and deeper to decode more things.
