package fluxdb

import (
	"encoding/hex"
	"encoding/json"
)

/*
 * VIVEMENT switcher les internals de `FluxDB` à des protobuf structs
 * C'est un gros hack pas scalable pour pouvoir décoder les internal des dbin files
 * avec `gob` (!!)
 */

type WriteRequest struct {
	ABIs []*ABIRow `json:"ABIs,omitempty"`

	AuthLinks   []*AuthLinkRow   `json:"AuthLinks,omitempty"`
	KeyAccounts []*KeyAccountRow `json:"KeyAccounts,omitempty"`
	TableDatas  []*TableDataRow  `json:"TableDatas,omitempty"`
	TableScopes []*TableScopeRow `json:"TableScopes,omitempty"`

	BlockNum uint32
	BlockID  HexBytes
}

type ABIRow struct {
	Account   uint64
	BlockNum  uint32 // in Read operation only
	PackedABI HexBytes
}

type AuthLinkRow struct {
	Deletion bool

	Account  uint64
	Contract uint64
	Action   uint64

	PermissionName uint64
}

type KeyAccountRow struct {
	PublicKey  string
	Account    uint64
	Permission uint64
	Deletion   bool
}

type TableDataRow struct {
	Account, Scope, Table, PrimKey uint64
	Payer                          uint64
	Deletion                       bool
	Data                           HexBytes
}

type TableScopeRow struct {
	Account, Scope, Table uint64
	Deletion              bool
	Payer                 uint64
}

type HexBytes []byte

func (t HexBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(t))
}
