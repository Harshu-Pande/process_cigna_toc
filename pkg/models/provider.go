package models

// TIN represents a Tax Identification Number with its type and value
type TIN struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

// ProviderGroup represents a group of providers with their NPIs and TIN information
type ProviderGroup struct {
	NPIs []string `json:"npi"`
	TIN  TIN      `json:"tin"`
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
