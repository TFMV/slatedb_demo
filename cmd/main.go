package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	pb "github.com/TFMV/slatedb_demo/proto"
	"github.com/fatih/color"
	"github.com/rodaine/table"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serverAddr = "localhost:5432"
	banner     = `
   _____ __      __       ____  ____ 
  / ___// /___ _/ /____  / __ \/ __ )
  \__ \/ / __ '/ __/ _ \/ / / / __  |
 ___/ / / /_/ / /_/  __/ /_/ / /_/ / 
/____/_/\__,_/\__/\___/_____/_____/  
                                     
A High-Performance Key-Value Store
`
	// ANSI color codes
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorPurple = "\033[35m"
	colorCyan   = "\033[36m"
	colorWhite  = "\033[37m"

	// Default server address
	defaultServerAddr = "localhost:5423"
)

var (
	// Color definitions
	titleColor   = color.New(color.FgHiCyan, color.Bold)
	successColor = color.New(color.FgHiGreen)
	errorColor   = color.New(color.FgHiRed)
	infoColor    = color.New(color.FgHiYellow)
	promptColor  = color.New(color.FgHiMagenta)
	keyColor     = color.New(color.FgHiBlue)
	valueColor   = color.New(color.FgHiWhite)
)

type SlateDBClient struct {
	client pb.SlateDBClient
	conn   *grpc.ClientConn
}

func NewSlateDBClient(serverAddr string) (*SlateDBClient, error) {
	// Set up connection to the gRPC server
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	// Create a new client
	client := pb.NewSlateDBClient(conn)
	return &SlateDBClient{
		client: client,
		conn:   conn,
	}, nil
}

func (c *SlateDBClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// Basic operations
func (c *SlateDBClient) Put(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.PutRequest{
		Key:   key,
		Value: value,
	}

	resp, err := c.client.Put(ctx, req)
	if err != nil {
		return err
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return nil
}

func (c *SlateDBClient) Get(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.GetRequest{
		Key: key,
	}

	resp, err := c.client.Get(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.Value == "" {
		infoColor.Printf("ℹ %s\n", resp.Message)
		return "", nil
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return resp.Value, nil
}

func (c *SlateDBClient) Delete(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.DeleteRequest{
		Key: key,
	}

	resp, err := c.client.Delete(ctx, req)
	if err != nil {
		return err
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return nil
}

// Batch operations
func (c *SlateDBClient) BatchPut(entries map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	keyValues := make([]*pb.KeyValue, 0, len(entries))
	for k, v := range entries {
		keyValues = append(keyValues, &pb.KeyValue{
			Key:   k,
			Value: v,
		})
	}

	req := &pb.BatchPutRequest{
		Entries: keyValues,
	}

	resp, err := c.client.BatchPut(ctx, req)
	if err != nil {
		return err
	}

	successColor.Printf("✓ %s (Success: %d, Failures: %d)\n",
		resp.Message, resp.SuccessCount, resp.FailureCount)
	return nil
}

func (c *SlateDBClient) BatchGet(keys []string) (map[string]string, []string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.BatchGetRequest{
		Keys: keys,
	}

	resp, err := c.client.BatchGet(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	results := make(map[string]string)
	for _, kv := range resp.Entries {
		results[kv.Key] = kv.Value
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return results, resp.MissingKeys, nil
}

func (c *SlateDBClient) BatchDelete(keys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.BatchDeleteRequest{
		Keys: keys,
	}

	resp, err := c.client.BatchDelete(ctx, req)
	if err != nil {
		return err
	}

	successColor.Printf("✓ %s (Success: %d, Failures: %d)\n",
		resp.Message, resp.SuccessCount, resp.FailureCount)
	return nil
}

// Scanning operations
func (c *SlateDBClient) PrefixScan(prefix string, limit int32) ([]*pb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.PrefixScanRequest{
		Prefix: prefix,
		Limit:  limit,
	}

	resp, err := c.client.PrefixScan(ctx, req)
	if err != nil {
		return nil, err
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return resp.Entries, nil
}

func (c *SlateDBClient) RangeScan(startKey, endKey string, limit int32) ([]*pb.KeyValue, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.RangeScanRequest{
		StartKey: startKey,
		EndKey:   endKey,
		Limit:    limit,
	}

	resp, err := c.client.RangeScan(ctx, req)
	if err != nil {
		return nil, err
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return resp.Entries, nil
}

// Statistics and monitoring
func (c *SlateDBClient) GetStats() (*pb.GetStatsResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.GetStatsRequest{}

	resp, err := c.client.GetStats(ctx, req)
	if err != nil {
		return nil, err
	}

	successColor.Printf("✓ %s\n", resp.Message)
	return resp, nil
}

// Helper functions for the CLI
func printBanner() {
	titleColor.Println(banner)
	fmt.Println()
	infoColor.Println("Welcome to the SlateDB CLI Demo!")
	fmt.Println()
}

func showMainMenu() int {
	titleColor.Println("\n=== SlateDB CLI Demo ===\n")
	fmt.Println("1. Basic Operations")
	fmt.Println("2. Batch Operations")
	fmt.Println("3. Scanning Operations")
	fmt.Println("4. Statistics")
	fmt.Println("5. Run Demo Scenario")
	fmt.Println("0. Exit")
	fmt.Println()

	return readIntInput("Choose an option: ")
}

func showBasicMenu() int {
	titleColor.Println("\n=== Basic Operations ===\n")
	fmt.Println("1. Put")
	fmt.Println("2. Get")
	fmt.Println("3. Delete")
	fmt.Println("0. Back to Main Menu")
	fmt.Println()

	return readIntInput("Choose an option: ")
}

func showBatchMenu() int {
	titleColor.Println("\n=== Batch Operations ===\n")
	fmt.Println("1. Batch Put")
	fmt.Println("2. Batch Get")
	fmt.Println("3. Batch Delete")
	fmt.Println("0. Back to Main Menu")
	fmt.Println()

	return readIntInput("Choose an option: ")
}

func showScanningMenu() int {
	titleColor.Println("\n=== Scanning Operations ===\n")
	fmt.Println("1. Prefix Scan")
	fmt.Println("2. Range Scan")
	fmt.Println("0. Back to Main Menu")
	fmt.Println()

	return readIntInput("Choose an option: ")
}

func readInput(prompt string) string {
	promptColor.Printf("%s: ", prompt)
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	return strings.TrimSpace(input)
}

func displayKeyValueTable(entries []*pb.KeyValue) {
	headerFmt := color.New(color.FgHiCyan, color.Underline).SprintfFunc()
	columnFmt := color.New(color.FgHiWhite).SprintfFunc()

	tbl := table.New("Key", "Value")
	tbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

	for _, entry := range entries {
		tbl.AddRow(entry.Key, entry.Value)
	}

	tbl.Print()
	fmt.Println()
}

func displayStatsTable(stats *pb.GetStatsResponse) {
	headerFmt := color.New(color.FgHiCyan, color.Underline).SprintfFunc()
	columnFmt := color.New(color.FgHiWhite).SprintfFunc()

	tbl := table.New("Metric", "Value")
	tbl.WithHeaderFormatter(headerFmt).WithFirstColumnFormatter(columnFmt)

	tbl.AddRow("Total Keys", stats.TotalKeys)
	tbl.AddRow("Total Size (bytes)", stats.TotalSizeBytes)
	tbl.AddRow("Database Path", stats.DbPath)

	tbl.Print()
	fmt.Println()
}

func handleBasicOperations(client *SlateDBClient) {
	for {
		choice := showBasicMenu()

		switch choice {
		case 1: // Put
			key := readInput("Enter key: ")
			if key == "" {
				errorColor.Println("Key cannot be empty")
				continue
			}

			value := readInput("Enter value: ")
			if value == "" {
				errorColor.Println("Value cannot be empty")
				continue
			}

			err := client.Put(key, value)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
			} else {
				successColor.Println("✓ Put operation successful")
			}

		case 2: // Get
			key := readInput("Enter key: ")
			if key == "" {
				errorColor.Println("Key cannot be empty")
				continue
			}

			value, err := client.Get(key)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
			} else {
				successColor.Println("✓ Get operation successful")
				fmt.Printf("Value: %s\n", value)
			}

		case 3: // Delete
			key := readInput("Enter key: ")
			if key == "" {
				errorColor.Println("Key cannot be empty")
				continue
			}

			err := client.Delete(key)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
			} else {
				successColor.Println("✓ Delete operation successful")
			}

		case 0: // Back to main menu
			return

		default:
			errorColor.Println("Invalid option. Please try again.")
		}
	}
}

func handleBatchOperations(client *SlateDBClient) {
	for {
		choice := showBatchMenu()

		switch choice {
		case 1: // Batch Put
			entries := make(map[string]string)

			count := readInput("How many key-value pairs to add")
			n, err := strconv.Atoi(count)
			if err != nil || n <= 0 {
				errorColor.Println("✗ Invalid number. Please enter a positive integer.")
				continue
			}

			for i := 1; i <= n; i++ {
				key := readInput(fmt.Sprintf("Enter key %d", i))
				value := readInput(fmt.Sprintf("Enter value %d", i))
				entries[key] = value
			}

			err = client.BatchPut(entries)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
			}

		case 2: // Batch Get
			keys := []string{}

			count := readInput("How many keys to retrieve")
			n, err := strconv.Atoi(count)
			if err != nil || n <= 0 {
				errorColor.Println("✗ Invalid number. Please enter a positive integer.")
				continue
			}

			for i := 1; i <= n; i++ {
				key := readInput(fmt.Sprintf("Enter key %d", i))
				keys = append(keys, key)
			}

			results, missing, err := client.BatchGet(keys)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
				continue
			}

			if len(results) > 0 {
				titleColor.Println("\n=== Retrieved Key-Value Pairs ===")
				entries := make([]*pb.KeyValue, 0, len(results))
				for k, v := range results {
					entries = append(entries, &pb.KeyValue{Key: k, Value: v})
				}
				displayKeyValueTable(entries)
			}

			if len(missing) > 0 {
				infoColor.Println("\n=== Missing Keys ===")
				for _, k := range missing {
					fmt.Printf("- %s\n", k)
				}
				fmt.Println()
			}

		case 3: // Batch Delete
			keys := []string{}

			count := readInput("How many keys to delete")
			n, err := strconv.Atoi(count)
			if err != nil || n <= 0 {
				errorColor.Println("✗ Invalid number. Please enter a positive integer.")
				continue
			}

			for i := 1; i <= n; i++ {
				key := readInput(fmt.Sprintf("Enter key %d", i))
				keys = append(keys, key)
			}

			err = client.BatchDelete(keys)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
			}

		case 0: // Back to main menu
			return

		default:
			errorColor.Println("Invalid option. Please try again.")
		}
	}
}

func handleScanningOperations(client *SlateDBClient) {
	for {
		choice := showScanningMenu()

		switch choice {
		case 1: // Prefix Scan
			prefix := readInput("Enter prefix")
			limitStr := readInput("Enter limit (or leave empty for default)")

			var limit int32 = 100
			if limitStr != "" {
				n, err := strconv.Atoi(limitStr)
				if err != nil || n <= 0 {
					errorColor.Println("✗ Invalid limit. Using default (100).")
				} else {
					limit = int32(n)
				}
			}

			entries, err := client.PrefixScan(prefix, limit)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
				continue
			}

			if len(entries) > 0 {
				titleColor.Printf("\n=== Keys with Prefix '%s' ===\n", prefix)
				displayKeyValueTable(entries)
			} else {
				infoColor.Printf("No keys found with prefix '%s'\n", prefix)
			}

		case 2: // Range Scan
			startKey := readInput("Enter start key (or leave empty for first key)")
			endKey := readInput("Enter end key (or leave empty for last key)")
			limitStr := readInput("Enter limit (or leave empty for default)")

			var limit int32 = 100
			if limitStr != "" {
				n, err := strconv.Atoi(limitStr)
				if err != nil || n <= 0 {
					errorColor.Println("✗ Invalid limit. Using default (100).")
				} else {
					limit = int32(n)
				}
			}

			entries, err := client.RangeScan(startKey, endKey, limit)
			if err != nil {
				errorColor.Printf("✗ Error: %v\n", err)
				continue
			}

			if len(entries) > 0 {
				titleColor.Printf("\n=== Keys in Range ['%s', '%s'] ===\n", startKey, endKey)
				displayKeyValueTable(entries)
			} else {
				infoColor.Printf("No keys found in range ['%s', '%s']\n", startKey, endKey)
			}

		case 0: // Back to main menu
			return

		default:
			errorColor.Println("Invalid option. Please try again.")
		}
	}
}

func handleStats(client *SlateDBClient) {
	stats, err := client.GetStats()
	if err != nil {
		errorColor.Printf("✗ Error: %v\n", err)
		return
	}

	titleColor.Println("\n=== SlateDB Statistics ===")
	displayStatsTable(stats)
}

func runDemoScenario(client *SlateDBClient) {
	titleColor.Println("\n=== Running Demo Scenario ===\n")

	// Step 1: Clear any existing data
	infoColor.Println("Step 1: Clearing existing demo data...")
	prefixEntries, err := client.PrefixScan("demo:", 100)
	if err != nil {
		errorColor.Printf("✗ Error scanning for demo data: %v\n", err)
		return
	}

	if len(prefixEntries) > 0 {
		keys := make([]string, len(prefixEntries))
		for i, entry := range prefixEntries {
			keys[i] = entry.Key
		}
		if err := client.BatchDelete(keys); err != nil {
			errorColor.Printf("✗ Error deleting demo data: %v\n", err)
			return
		}
	}

	// Step 2: Insert some data
	infoColor.Println("\nStep 2: Inserting sample data...")
	demoData := map[string]string{
		"demo:user:1":    `{"id": 1, "name": "Alice", "email": "alice@example.com"}`,
		"demo:user:2":    `{"id": 2, "name": "Bob", "email": "bob@example.com"}`,
		"demo:user:3":    `{"id": 3, "name": "Charlie", "email": "charlie@example.com"}`,
		"demo:product:1": `{"id": 1, "name": "Laptop", "price": 999.99}`,
		"demo:product:2": `{"id": 2, "name": "Phone", "price": 699.99}`,
		"demo:order:1":   `{"id": 1, "user_id": 1, "product_id": 1, "quantity": 1}`,
		"demo:order:2":   `{"id": 2, "user_id": 2, "product_id": 2, "quantity": 2}`,
	}
	if err := client.BatchPut(demoData); err != nil {
		errorColor.Printf("✗ Error inserting sample data: %v\n", err)
		return
	}

	// Step 3: Retrieve and display all data
	infoColor.Println("\nStep 3: Retrieving all data with prefix scan...")
	time.Sleep(1 * time.Second)
	allEntries, err := client.PrefixScan("demo:", 100)
	if err != nil {
		errorColor.Printf("✗ Error retrieving all data: %v\n", err)
		return
	}
	displayKeyValueTable(allEntries)

	// Step 4: Retrieve users
	infoColor.Println("\nStep 4: Retrieving only users...")
	time.Sleep(1 * time.Second)
	userEntries, err := client.PrefixScan("demo:user:", 100)
	if err != nil {
		errorColor.Printf("✗ Error retrieving users: %v\n", err)
		return
	}
	displayKeyValueTable(userEntries)

	// Step 5: Retrieve products
	infoColor.Println("\nStep 5: Retrieving only products...")
	time.Sleep(1 * time.Second)
	productEntries, err := client.PrefixScan("demo:product:", 100)
	if err != nil {
		errorColor.Printf("✗ Error retrieving products: %v\n", err)
		return
	}
	displayKeyValueTable(productEntries)

	// Step 6: Retrieve orders
	infoColor.Println("\nStep 6: Retrieving only orders...")
	time.Sleep(1 * time.Second)
	orderEntries, err := client.PrefixScan("demo:order:", 100)
	if err != nil {
		errorColor.Printf("✗ Error retrieving orders: %v\n", err)
		return
	}
	displayKeyValueTable(orderEntries)

	// Step 7: Get statistics
	infoColor.Println("\nStep 7: Getting database statistics...")
	time.Sleep(1 * time.Second)
	stats, err := client.GetStats()
	if err != nil {
		errorColor.Printf("✗ Error getting stats: %v\n", err)
	} else if stats != nil {
		displayStatsTable(stats)
	} else {
		errorColor.Println("✗ No statistics available")
	}

	// Step 8: Delete a user
	infoColor.Println("\nStep 8: Deleting user 2...")
	time.Sleep(1 * time.Second)
	if err := client.Delete("demo:user:2"); err != nil {
		errorColor.Printf("✗ Error deleting user: %v\n", err)
		return
	}

	// Step 9: Verify deletion
	infoColor.Println("\nStep 9: Verifying deletion with batch get...")
	time.Sleep(1 * time.Second)
	results, missing, err := client.BatchGet([]string{"demo:user:1", "demo:user:2", "demo:user:3"})
	if err != nil {
		errorColor.Printf("✗ Error verifying deletion: %v\n", err)
		return
	}

	if len(results) > 0 {
		titleColor.Println("\n=== Existing Users ===")
		entries := make([]*pb.KeyValue, 0, len(results))
		for k, v := range results {
			entries = append(entries, &pb.KeyValue{Key: k, Value: v})
		}
		displayKeyValueTable(entries)
	}

	if len(missing) > 0 {
		titleColor.Println("\n=== Missing Users ===")
		for _, k := range missing {
			fmt.Printf("- %s\n", k)
		}
		fmt.Println()
	}

	// Step 10: Range scan
	infoColor.Println("\nStep 10: Performing range scan from 'demo:order:1' to 'demo:order:3'...")
	time.Sleep(1 * time.Second)
	rangeEntries, err := client.RangeScan("demo:order:1", "demo:order:3", 100)
	if err != nil {
		errorColor.Printf("✗ Error performing range scan: %v\n", err)
		return
	}
	displayKeyValueTable(rangeEntries)

	successColor.Println("\n✓ Demo scenario completed successfully!")
}

func main() {
	// Print banner
	fmt.Println(banner)

	// Get server address from environment variable or use default
	serverAddr := defaultServerAddr
	if envAddr := os.Getenv("SERVER_ADDR"); envAddr != "" {
		serverAddr = envAddr
	}

	// Create a new client
	client, err := NewSlateDBClient(serverAddr)
	if err != nil {
		errorColor.Printf("Failed to create SlateDB client: %v\n", err)
		errorColor.Println("\nTroubleshooting tips:")
		errorColor.Println("1. Make sure the SlateDB server is running")
		errorColor.Println("2. Check if the server address is correct (current: " + serverAddr + ")")
		errorColor.Println("3. You can set a custom server address with the SERVER_ADDR environment variable")
		errorColor.Println("   Example: SERVER_ADDR=localhost:8080 go run main.go")
		os.Exit(1)
	}
	defer client.Close()

	// Check connection to server
	infoColor.Printf("Connecting to SlateDB server at %s...\n", serverAddr)
	if err := checkConnection(client); err != nil {
		errorColor.Printf("Failed to connect to SlateDB server: %v\n", err)
		errorColor.Println("\nTroubleshooting tips:")
		errorColor.Println("1. Make sure the SlateDB server is running")
		errorColor.Println("2. Check if the server is listening on port " + strings.Split(serverAddr, ":")[1])
		errorColor.Println("3. Verify there are no firewall rules blocking the connection")
		errorColor.Println("4. You can set a custom server address with the SERVER_ADDR environment variable")
		errorColor.Println("   Example: SERVER_ADDR=localhost:8080 go run main.go")
		os.Exit(1)
	}
	successColor.Println("✓ Successfully connected to SlateDB server!")

	// Main menu loop
	for {
		choice := showMainMenu()
		switch choice {
		case 1:
			handleBasicOperations(client)
		case 2:
			handleBatchOperations(client)
		case 3:
			handleScanningOperations(client)
		case 4:
			handleStats(client)
		case 5:
			runDemoScenario(client)
		case 0, 6:
			successColor.Println("Exiting SlateDB CLI. Goodbye!")
			return
		default:
			errorColor.Println("Invalid option. Please try again.")
		}
	}
}

// checkConnection verifies that the server is reachable
func checkConnection(client *SlateDBClient) error {
	// Try to get stats as a simple ping
	_, err := client.GetStats()
	return err
}

// readIntInput reads an integer input from the user
func readIntInput(prompt string) int {
	for {
		promptColor.Print(prompt)
		input := readInput("")

		// Handle empty input
		if input == "" {
			continue
		}

		// Handle exit command
		if input == "exit" || input == "quit" || input == "q" {
			successColor.Println("Exiting SlateDB CLI. Goodbye!")
			os.Exit(0)
		}

		// Convert to integer
		choice, err := strconv.Atoi(input)
		if err != nil {
			errorColor.Println("Please enter a valid number.")
			continue
		}

		return choice
	}
}
