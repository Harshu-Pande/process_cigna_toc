package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"provider-extractor/pkg/processor"
)

func main() {
	// Parse command line flags
	csvPath := flag.String("csv", "", "Path to CSV file containing URLs")
	outputDir := flag.String("output", "extracted_providers", "Output directory for extracted provider references")
	flag.Parse()

	if *csvPath == "" {
		fmt.Println("Error: CSV file path is required")
		flag.Usage()
		os.Exit(1)
	}

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	// Open CSV file
	file, err := os.Open(*csvPath)
	if err != nil {
		fmt.Printf("Error opening CSV file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Create CSV reader
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Error reading CSV file: %v\n", err)
		os.Exit(1)
	}

	// Create extractor
	extractor, err := processor.NewExtractor(*outputDir)
	if err != nil {
		fmt.Printf("Error creating extractor: %v\n", err)
		os.Exit(1)
	}

	// Process each URL
	for i, record := range records {
		if i == 0 || len(record) == 0 { // Skip header row
			continue
		}

		url := record[0]
		fmt.Printf("\nProcessing URL %d/%d: %s\n", i, len(records)-1, url)

		if err := extractor.ProcessURL(url); err != nil {
			fmt.Printf("Error processing URL: %v\n", err)
			continue
		}
	}

	fmt.Println("\nProcessing complete!")
}
