# Cigna Provider Reference Extractor

This tool extracts provider references from Cigna's in-network rate files, supporting multiple file formats (JSON, GZIP, ZIP).

## Google Colab Instructions

Follow these steps to run the code in Google Colab. Each section represents a cell you can copy and paste directly into Colab.

### 1. Clone the Repository
```bash
!git clone https://github.com/Harshu-Pande/process_cigna_toc.git
%cd process_cigna_toc
```

### 2. Set up Go Environment
```bash
!apt-get update
!apt-get install -y golang
!go version
```

### 3. Initialize Go Module
```bash
!go mod init provider-extractor
!go mod tidy
```

### 4. Run the Extractor
```bash
!go run cmd/extract/main.go -csv clean_in_network_locations.csv
```

## Output

The program will:
1. Process each URL in the CSV file
2. Extract provider references
3. Save them to JSON files in the `extracted_providers` directory

Each output file will contain:
- Provider group IDs
- NPIs (National Provider Identifiers)
- TIN (Tax Identification Number) information

## Supported File Formats

The tool can handle:
1. Regular JSON files
2. Gzipped JSON files (.gz)
3. ZIP files containing JSON files (.zip)

## File Structure

```
.
├── cmd/
│   └── extract/
│       └── main.go          # Main program entry point
├── pkg/
│   ├── models/
│   │   └── provider.go      # Data models
│   └── processor/
│       └── extractor.go     # Core extraction logic
├── clean_in_network_locations.csv  # Input URLs
├── go.mod                   # Go module file
└── README.md               # This file
```

## Example Output

The extracted provider references will be in this format:
```json
[
  {
    "provider_group_id": 1,
    "provider_groups": [
      {
        "npi": ["1234567890"],
        "tin": {
          "type": "ein",
          "value": "123456789"
        }
      }
    ]
  }
]
``` 