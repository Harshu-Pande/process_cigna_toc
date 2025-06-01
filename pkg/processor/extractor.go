package processor

import (
	"archive/zip"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"provider-extractor/pkg/models"
)

// Extractor handles the extraction of provider references from files
type Extractor struct {
	outputDir string
	client    *http.Client
}

// NewExtractor creates a new Extractor instance
func NewExtractor(outputDir string) (*Extractor, error) {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	// Create HTTP client with improved configuration
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			DisableCompression:  false,
			ForceAttemptHTTP2:   true,
		},
	}

	return &Extractor{
		outputDir: outputDir,
		client:    client,
	}, nil
}

// ProcessURL downloads and processes a URL
func (e *Extractor) ProcessURL(url string) error {
	fmt.Printf("Processing URL: %s\n", url)

	// Create request with browser-like headers
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}

	// Add headers to mimic a browser, but don't request gzip encoding
	// since we'll handle it ourselves based on file extension
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Connection", "keep-alive")

	// Make the request
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to download file: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
	}

	// Read the entire response for ZIP files
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	var reader io.Reader

	// Check file extension (ignoring query parameters)
	fileURL := strings.Split(url, "?")[0]
	switch {
	case strings.HasSuffix(fileURL, ".gz"):
		// Handle gzip
		gzReader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gzReader.Close()
		reader = gzReader

	case strings.HasSuffix(fileURL, ".zip"):
		// Handle ZIP
		zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
		if err != nil {
			return fmt.Errorf("failed to create zip reader: %v", err)
		}

		// Find the first JSON file in the ZIP
		var jsonFile *zip.File
		for _, file := range zipReader.File {
			if strings.HasSuffix(strings.ToLower(file.Name), ".json") {
				jsonFile = file
				break
			}
		}

		if jsonFile == nil {
			return fmt.Errorf("no JSON file found in ZIP archive")
		}

		// Open the JSON file from the ZIP
		rc, err := jsonFile.Open()
		if err != nil {
			return fmt.Errorf("failed to open JSON file from ZIP: %v", err)
		}
		defer rc.Close()

		// Read the JSON content
		jsonContent, err := io.ReadAll(rc)
		if err != nil {
			return fmt.Errorf("failed to read JSON from ZIP: %v", err)
		}
		reader = bytes.NewReader(jsonContent)

	default:
		// Regular file
		reader = bytes.NewReader(body)
	}

	// Create a decoder that uses json.Number for numbers
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()

	// Determine format based on URL
	if strings.Contains(url, "oscar") {
		return e.processOscarFormat(decoder, url)
	} else {
		return e.processCignaFormat(decoder, url)
	}
}

// ProcessFile processes a file and extracts provider references
func (e *Extractor) ProcessFile(filePath string) error {
	// Check if the input is a URL
	if strings.HasPrefix(filePath, "http://") || strings.HasPrefix(filePath, "https://") {
		return e.ProcessURL(filePath)
	}

	fmt.Printf("Processing file: %s\n", filePath)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	var reader io.Reader = file

	// If file has .gz extension, create a gzip reader
	if strings.HasSuffix(filePath, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// Create a decoder that uses json.Number for numbers
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()

	// Determine format based on filename
	if strings.Contains(filePath, "oscar") {
		return e.processOscarFormat(decoder, filePath)
	} else {
		return e.processCignaFormat(decoder, filePath)
	}
}

func (e *Extractor) processOscarFormat(decoder *json.Decoder, source string) error {
	var providers []models.ProviderReference
	var nextID int = 1

	// Read opening token which should be '{'
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening token: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object start, got %v", t)
	}

	// Process object fields until we find in_network
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}

		if key, ok := t.(string); ok && key == "in_network" {
			// Found in_network field, expect array start
			t, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read in_network start: %v", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return fmt.Errorf("expected array start for in_network, got %v", t)
			}

			// Process array elements
			for decoder.More() {
				var item struct {
					NegotiatedRates []struct {
						ProviderGroups []models.ProviderGroup `json:"provider_groups"`
					} `json:"negotiated_rates"`
				}

				if err := decoder.Decode(&item); err != nil {
					return fmt.Errorf("failed to decode array item: %v", err)
				}

				// Extract provider groups from negotiated rates
				for _, rate := range item.NegotiatedRates {
					if len(rate.ProviderGroups) > 0 {
						providers = append(providers, models.ProviderReference{
							ProviderGroupID: nextID,
							ProviderGroups:  rate.ProviderGroups,
						})
						nextID++
					}
				}
			}

			return e.saveProviderReferences(providers, source)
		} else {
			// Skip other fields
			if _, err := decoder.Token(); err != nil {
				return fmt.Errorf("failed to skip field: %v", err)
			}
		}
	}

	return fmt.Errorf("in_network field not found")
}

// fetchRemoteReferences fetches a batch of remote provider references
func (e *Extractor) fetchRemoteReferences(refs []models.RemoteProviderReference) ([]models.ProviderReference, []error) {
	// Create a transport with optimized settings
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
		DisableCompression:  false,
		ForceAttemptHTTP2:   true,
	}

	// Create a client with the optimized transport
	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// Create channels for results and errors
	results := make(chan models.ProviderReference, len(refs))
	errors := make(chan error, len(refs))

	// Create a worker pool
	maxWorkers := 50 // Increased from 10 to 50
	sem := make(chan struct{}, maxWorkers)

	// Start workers
	var wg sync.WaitGroup
	for _, ref := range refs {
		wg.Add(1)
		go func(ref models.RemoteProviderReference) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			// Create request
			req, err := http.NewRequest("GET", ref.Location, nil)
			if err != nil {
				errors <- fmt.Errorf("failed to create request for %d: %v", ref.ProviderGroupID, err)
				return
			}

			// Add headers
			req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
			req.Header.Set("Accept", "*/*")
			req.Header.Set("Connection", "keep-alive")

			// Make the request
			resp, err := client.Do(req)
			if err != nil {
				errors <- fmt.Errorf("failed to fetch %d: %v", ref.ProviderGroupID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				errors <- fmt.Errorf("received non-200 status code for %d: %d", ref.ProviderGroupID, resp.StatusCode)
				return
			}

			// Decode the response
			var provRef models.ProviderReference
			if err := json.NewDecoder(resp.Body).Decode(&provRef); err != nil {
				errors <- fmt.Errorf("failed to decode %d: %v", ref.ProviderGroupID, err)
				return
			}

			// Set the provider group ID
			provRef.ProviderGroupID = ref.ProviderGroupID
			results <- provRef
		}(ref)
	}

	// Start a goroutine to close channels when done
	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	// Collect results
	var providers []models.ProviderReference
	var errs []error

	// Use a ticker for progress updates
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	processed := 0
	total := len(refs)

	// Create a buffer for providers to reduce memory allocations
	providers = make([]models.ProviderReference, 0, total)

	// Process results as they come in
	for {
		select {
		case result, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			providers = append(providers, result)
			processed++
		case err, ok := <-errors:
			if !ok {
				errors = nil
				continue
			}
			errs = append(errs, err)
			processed++
		case <-ticker.C:
			if processed > 0 {
				fmt.Printf("Progress: %d/%d references (%.1f%%) processed...\n",
					processed, total, float64(processed)/float64(total)*100)
			}
		}

		// Break when both channels are closed
		if results == nil && errors == nil {
			break
		}
	}

	return providers, errs
}

func (e *Extractor) processCignaFormat(decoder *json.Decoder, source string) error {
	// Read opening token which should be '{'
	t, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening token: %v", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object start, got %v", t)
	}

	// Process object fields until we find provider_references
	for decoder.More() {
		t, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read token: %v", err)
		}

		if key, ok := t.(string); ok && key == "provider_references" {
			// Found provider_references field, expect array start
			t, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to read provider_references start: %v", err)
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				return fmt.Errorf("expected array start for provider_references, got %v", t)
			}

			var remoteRefs []models.RemoteProviderReference
			for decoder.More() {
				var remoteRef models.RemoteProviderReference
				if err := decoder.Decode(&remoteRef); err != nil {
					return fmt.Errorf("failed to decode provider reference: %v", err)
				}
				remoteRefs = append(remoteRefs, remoteRef)
			}

			fmt.Printf("Found %d remote provider references. Fetching data...\n", len(remoteRefs))

			// Fetch all references
			providers, errors := e.fetchRemoteReferences(remoteRefs)

			// Log any errors but continue
			if len(errors) > 0 {
				fmt.Printf("Warning: Failed to fetch %d remote references:\n", len(errors))
				for _, err := range errors {
					fmt.Printf("  %v\n", err)
				}
			}

			return e.saveProviderReferences(providers, source)
		} else {
			// Skip other fields
			if _, err := decoder.Token(); err != nil {
				return fmt.Errorf("failed to skip field: %v", err)
			}
		}
	}

	return fmt.Errorf("provider_references field not found")
}

func (e *Extractor) saveProviderReferences(providers []models.ProviderReference, sourceFile string) error {
	// Create output filename based on source file
	baseName := filepath.Base(sourceFile)
	baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	baseName = strings.TrimSuffix(baseName, ".json") // Remove .json if present before .gz

	// Clean the basename for URLs
	baseName = strings.Split(baseName, "?")[0] // Remove query parameters

	outputFile := filepath.Join(e.outputDir, fmt.Sprintf("%s_provider_references.json", baseName))

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Create JSON encoder with pretty printing
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	// Write providers to file
	if err := encoder.Encode(providers); err != nil {
		return fmt.Errorf("failed to write provider references: %v", err)
	}

	fmt.Printf("Successfully extracted %d provider references to: %s\n", len(providers), outputFile)
	return nil
}
