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
	"runtime"
	"strings"
	"sync"
	"time"

	"provider-extractor/pkg/models"
)

// Extractor handles the extraction of provider references from files
type Extractor struct {
	outputDir string
	client    *http.Client
	// Configuration for concurrency
	maxURLWorkers      int
	maxProviderWorkers int
	maxConcurrentFiles int
	maxRetriesPerURL   int
}

// NewExtractor creates a new Extractor instance
func NewExtractor(outputDir string) (*Extractor, error) {
	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %v", err)
	}

	// Calculate optimal concurrency based on available CPU cores
	numCPU := runtime.NumCPU()

	// Create HTTP client with improved configuration for large files
	client := &http.Client{
		Timeout: 5 * time.Minute, // Increased timeout for large files
		Transport: &http.Transport{
			MaxIdleConns:        numCPU * 10, // Scale with CPU count
			MaxIdleConnsPerHost: numCPU * 10,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
			DisableCompression:  false,
			ForceAttemptHTTP2:   true,
		},
	}

	return &Extractor{
		outputDir:          outputDir,
		client:             client,
		maxURLWorkers:      numCPU,     // Process URLs concurrently
		maxProviderWorkers: numCPU * 4, // More workers for provider fetching
		maxConcurrentFiles: 10,         // Limit concurrent file operations
		maxRetriesPerURL:   3,          // Maximum retries per URL
	}, nil
}

// ProcessURLs processes multiple URLs concurrently
func (e *Extractor) ProcessURLs(urls []string) error {
	fmt.Printf("Processing %d URLs using %d workers...\n", len(urls), e.maxURLWorkers)

	// Create channels for work distribution and error collection
	urlChan := make(chan string, len(urls))
	errorChan := make(chan error, len(urls))
	var wg sync.WaitGroup

	// Create semaphore to limit concurrent file operations
	fileSem := make(chan struct{}, e.maxConcurrentFiles)

	// Start worker pool
	for i := 0; i < e.maxURLWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for url := range urlChan {
				// Acquire semaphore before file operations
				fileSem <- struct{}{}
				err := e.ProcessURL(url)
				<-fileSem // Release semaphore

				if err != nil {
					errorChan <- fmt.Errorf("worker %d failed processing %s: %v", workerID, url, err)
				}
			}
		}(i)
	}

	// Feed URLs to workers
	for _, url := range urls {
		urlChan <- url
	}
	close(urlChan)

	// Wait for all workers to finish
	wg.Wait()
	close(errorChan)

	// Collect and report errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		fmt.Printf("\nEncountered %d errors while processing URLs:\n", len(errors))
		for _, err := range errors {
			fmt.Printf("  %v\n", err)
		}
	}

	fmt.Printf("\nProcessing complete! Successfully processed %d/%d URLs\n", len(urls)-len(errors), len(urls))
	return nil
}

// downloadWithRetry attempts to download a URL with retries
func (e *Extractor) downloadWithRetry(url string, maxRetries int) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 2, 4, 8 seconds
			backoff := time.Duration(1<<uint(attempt)) * time.Second
			time.Sleep(backoff)
			fmt.Printf("Retry attempt %d/%d for URL: %s\n", attempt, maxRetries, url)
		}

		// Create request with browser-like headers
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %v", err)
			continue
		}

		// Add headers to mimic a browser
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Encoding", "gzip, deflate")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")

		// Make the request
		resp, err := e.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to download file: %v", err)
			continue
		}

		// Check status code
		if resp.StatusCode == http.StatusForbidden {
			resp.Body.Close()
			lastErr = fmt.Errorf("access denied (403) - URL may require authentication or be expired")
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			lastErr = fmt.Errorf("received non-200 status code: %d", resp.StatusCode)
			continue
		}

		// Read body with progress reporting for large files
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body: %v", err)
			continue
		}

		return body, nil
	}

	return nil, fmt.Errorf("failed after %d retries: %v", maxRetries, lastErr)
}

// ProcessURL downloads and processes a URL
func (e *Extractor) ProcessURL(url string) error {
	fmt.Printf("Processing URL: %s\n", url)

	// Download file with retries
	body, err := e.downloadWithRetry(url, 3)
	if err != nil {
		return err
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
	results := make(chan models.ProviderReference, len(refs))
	errors := make(chan error, len(refs))

	// Create a worker pool with increased size
	sem := make(chan struct{}, e.maxProviderWorkers)

	// Progress tracking
	var processed int64
	var mu sync.Mutex
	total := len(refs)
	lastProgress := time.Now()
	reportProgress := func() {
		mu.Lock()
		processed++
		if time.Since(lastProgress) >= 5*time.Second {
			fmt.Printf("Progress: %d/%d references (%.1f%%) processed...\n",
				processed, total, float64(processed)/float64(total)*100)
			lastProgress = time.Now()
		}
		mu.Unlock()
	}

	// Start workers
	var wg sync.WaitGroup
	for _, ref := range refs {
		wg.Add(1)
		go func(ref models.RemoteProviderReference) {
			defer wg.Done()
			defer reportProgress()

			// Validate URL before processing
			if ref.Location == "" {
				errors <- fmt.Errorf("empty location URL for provider group %d", ref.ProviderGroupID)
				return
			}

			if !strings.HasPrefix(ref.Location, "http://") && !strings.HasPrefix(ref.Location, "https://") {
				errors <- fmt.Errorf("invalid URL scheme for provider group %d: %s", ref.ProviderGroupID, ref.Location)
				return
			}

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
			req.Header.Set("Accept-Encoding", "gzip, deflate")
			req.Header.Set("Connection", "keep-alive")
			req.Header.Set("Cache-Control", "no-cache")

			// Make the request with retries
			var resp *http.Response
			var lastErr error
			for attempt := 0; attempt < e.maxRetriesPerURL; attempt++ {
				if attempt > 0 {
					time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
				}
				resp, err = e.client.Do(req)
				if err == nil && resp.StatusCode == http.StatusOK {
					break
				}
				if resp != nil {
					resp.Body.Close()
				}
				lastErr = err
			}
			if lastErr != nil {
				errors <- fmt.Errorf("failed to fetch %d after %d retries: %v", ref.ProviderGroupID, e.maxRetriesPerURL, lastErr)
				return
			}
			defer resp.Body.Close()

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

	// Pre-allocate slices for better performance
	providers = make([]models.ProviderReference, 0, len(refs))
	errs = make([]error, 0)

	// Process results as they come in
	for {
		select {
		case result, ok := <-results:
			if !ok {
				results = nil
				continue
			}
			providers = append(providers, result)
		case err, ok := <-errors:
			if !ok {
				errors = nil
				continue
			}
			errs = append(errs, err)
		}

		// Break when both channels are closed
		if results == nil && errors == nil {
			break
		}
	}

	// Group and summarize errors by type
	if len(errs) > 0 {
		errorsByType := make(map[string][]int)
		for _, err := range errs {
			errType := "other"
			if strings.Contains(err.Error(), "empty location URL") {
				errType = "empty_url"
			} else if strings.Contains(err.Error(), "invalid URL scheme") {
				errType = "invalid_scheme"
			} else if strings.Contains(err.Error(), "failed to fetch") {
				errType = "fetch_failed"
			} else if strings.Contains(err.Error(), "failed to decode") {
				errType = "decode_failed"
			}

			// Extract provider group ID from error message
			var id int
			if _, err := fmt.Sscanf(err.Error(), "failed to fetch %d", &id); err == nil {
				errorsByType[errType] = append(errorsByType[errType], id)
			} else if _, err := fmt.Sscanf(err.Error(), "empty location URL for provider group %d", &id); err == nil {
				errorsByType[errType] = append(errorsByType[errType], id)
			} else if _, err := fmt.Sscanf(err.Error(), "invalid URL scheme for provider group %d", &id); err == nil {
				errorsByType[errType] = append(errorsByType[errType], id)
			}
		}

		// Print error summary
		fmt.Printf("\nError Summary:\n")
		for errType, ids := range errorsByType {
			switch errType {
			case "empty_url":
				fmt.Printf("- %d references had empty URLs\n", len(ids))
			case "invalid_scheme":
				fmt.Printf("- %d references had invalid URL schemes\n", len(ids))
			case "fetch_failed":
				fmt.Printf("- %d references failed to fetch\n", len(ids))
			case "decode_failed":
				fmt.Printf("- %d references failed to decode\n", len(ids))
			default:
				fmt.Printf("- %d other errors\n", len(ids))
			}
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
