package deth

import (
	"fmt"
	"math/big"

	"github.com/eoscanada/doh/jsonpb"
)

func (m *BigInt) MarshalJSONPB(*jsonpb.Marshaler) ([]byte, error) {
	z := new(big.Int)
	z.SetBytes(m.Bytes)
	return []byte(fmt.Sprintf(`"%s"`, z.String())), nil
}
