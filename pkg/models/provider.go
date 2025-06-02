package models

import (
	"encoding/json"
	"fmt"
)

// TIN represents a Tax Identification Number with its type and value
type TIN struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// ProviderGroup represents a group of providers with their NPIs and TIN information
type ProviderGroup struct {
	NPIs []interface{} `json:"npi"` // Can be either string or number
	TIN  TIN           `json:"tin"`
}

// ProviderReference represents a provider reference with its group ID and provider groups
type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
}

// RemoteProviderReference represents a remote provider reference with its group ID and location
type RemoteProviderReference struct {
	ProviderGroupID int    `json:"provider_group_id"`
	Location        string `json:"location"`
}

// ConvertNPIToString converts an NPI value (which can be string or number) to string
func ConvertNPIToString(npi interface{}) string {
	switch v := npi.(type) {
	case string:
		return v
	case json.Number:
		return string(v)
	case float64:
		return json.Number(fmt.Sprintf("%.0f", v)).String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
