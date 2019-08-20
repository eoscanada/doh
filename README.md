pbop - pry open those protobuf files
------------------------------------

pbop is aware of our block structures, of dbin packing and can render most of the things to JSON
with decent unpacking.

Install with:

    go get -u github.com/eoscanada/pbop


Usage:

__pbop bt ls__

```shell script
$ pbop bt ls --db test:dev 
Listing tables:
- eth-test-v1-trxs
- eth-test-v1-timeline
- eth-test-v1-blocks
```

__pbop bt read__
```shell script
$ pbop bt read eth-test-v1-trxs --db test:dev --type ETH --prefix trx:000170ffbb87f07ae38e505a14e5754a4eee028fe8eac217d34a1c9d112bf89b:00000000007fffc6:360131db -d 0
{...}

$ pbop bt read eth-test-v1-trxs --db test:dev --type ETH
{...}
{...}
{...}
```

```shell script
$ pbop -t bstream.v1.Block -i ../search/testdata/eth/02-block-with-logs.dat -d 1 | jq . | less
{...}

$ pbop dbin -d 0 oneblock.eth.dbin | jq . | less
{...}
{...}
```

The `-d` flag represents the depth of decoding.. when decoding known
structures, we can go deeper and deeper to decode more things.
