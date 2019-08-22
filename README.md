doh - dfuse operations helpers, the kitchen sink of tools
---------------------------------------------------------

doh is aware of our block structures, of dbin packing and can render most of the things to JSON
with decent unpacking.

Install with:

    go get -u github.com/eoscanada/doh


Usage:

__doh bt ls__

```shell script
$ doh bt ls --db test:dev
Listing tables:
- eth-test-v1-trxs
- eth-test-v1-timeline
- eth-test-v1-blocks
```

__doh bt read__
```shell script
$ doh bt read eth-test-v1-trxs --db test:dev --type ETH --prefix trx:000170ffbb87f07ae38e505a14e5754a4eee028fe8eac217d34a1c9d112bf89b:00000000007fffc6:360131db -d 0
{...}

$ doh bt read eth-test-v1-trxs --db test:dev --type ETH
{...}
{...}
{...}
```

```shell script
$ doh -t bstream.v1.Block -i ../search/testdata/eth/02-block-with-logs.dat -d 1 | jq . | less
{...}

$ doh dbin -d 0 oneblock.eth.dbin | jq . | less
{...}
{...}
```

The `-d` flag represents the depth of decoding.. when decoding known
structures, we can go deeper and deeper to decode more things.
