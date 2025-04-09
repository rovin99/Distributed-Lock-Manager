package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"Distributed-Lock-Manager/internal/client"
)

func main() {
	// Add debug for command-line arguments
	fmt.Println("DEBUG - Raw command-line args:", os.Args)

	// Define command-line flags
	var (
		serversFlag = flag.String("servers", "localhost:50051", "Comma-separated list of server addresses")
		clientID    = flag.Int("client-id", 1, "Client ID")
		timeout     = flag.Duration("timeout", 60*time.Second, "Timeout for the operation")
		filename    = flag.String("file", "file_0", "File to append to (when using append command)")
		content     = flag.String("content", "test content", "Content to append to file (when using append command)")
		repeat      = flag.Int("repeat", 1, "Number of times to repeat the operation")
		interval    = flag.Duration("interval", 5*time.Second, "Interval between repeated operations")
	)

	// Custom flag parsing to handle command in the middle
	var command string
	var cmdArgs []string
	var cmdPos int

	// Find the position of the command (acquire, release, append, hold)
	for i, arg := range os.Args {
		if arg == "acquire" || arg == "release" || arg == "append" || arg == "hold" {
			command = arg
			cmdPos = i
			break
		}
	}

	// If we found a command, split arguments
	if command != "" {
		// Parse flags that come before the command
		flag.CommandLine.Parse(os.Args[1:cmdPos])

		// Parse flags that come after the command
		if cmdPos+1 < len(os.Args) {
			cmdArgs = os.Args[cmdPos+1:]
			flag.CommandLine.Parse(cmdArgs)
		}
	} else {
		// Standard flag parsing if no command found
		flag.Parse()

		if flag.NArg() < 1 {
			flag.Usage()
			os.Exit(1)
		}
		command = flag.Arg(0)
	}

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] command [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Commands:\n")
		fmt.Fprintf(os.Stderr, "  acquire    Acquire a lock\n")
		fmt.Fprintf(os.Stderr, "  release    Release a lock\n")
		fmt.Fprintf(os.Stderr, "  append     Append to a file (requires lock)\n")
		fmt.Fprintf(os.Stderr, "  hold       Acquire a lock and hold it until program is terminated\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}

	// Debug flags after parsing
	fmt.Printf("DEBUG - After flag.Parse(): servers=%s, command=%s\n", *serversFlag, command)
	fmt.Printf("DEBUG - FLAGS: client-id=%d, file=%s, content=%s, timeout=%v\n",
		*clientID, *filename, *content, *timeout)

	// Parse the server addresses
	serverAddrs := strings.Split(*serversFlag, ",")

	// Add debugging output
	fmt.Printf("INFO: Using server addresses: %v\n", serverAddrs)

	// Create a client with the specified server addresses for failover
	c, err := client.NewLockClientWithFailover(serverAddrs, int32(*clientID))
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	// Initialize the client session
	if err := c.ClientInit(); err != nil {
		fmt.Printf("Failed to initialize client: %v\n", err)
		os.Exit(1)
	}

	// Set up a timeout for the overall operation
	timeoutCh := time.After(*timeout)
	done := make(chan struct{})

	// Execute the command based on the input
	go func() {
		for i := 0; i < *repeat; i++ {
			if i > 0 {
				fmt.Printf("Sleeping for %v before next operation...\n", *interval)
				time.Sleep(*interval)
			}

			switch command {
			case "acquire":
				fmt.Printf("Acquiring lock...\n")
				err := c.AcquireLockWithRetry()
				if err != nil {
					fmt.Printf("Failed to acquire lock: %v\n", err)
					close(done)
					return
				}
				fmt.Printf("Successfully acquired lock\n")

				if *repeat == 1 {
					// If not repeating, hold the lock until timeout
					fmt.Printf("Lock acquired. Holding until timeout or interrupt...\n")
					// Keep the lock until the program is terminated
					select {
					case <-timeoutCh:
						fmt.Printf("Timeout reached, releasing lock...\n")
						c.LockRelease()
						close(done)
						return
					}
				} else {
					// If repeating, release the lock at the end of each iteration
					if err := c.LockRelease(); err != nil {
						fmt.Printf("Failed to release lock: %v\n", err)
					} else {
						fmt.Printf("Lock released\n")
					}
				}

			case "release":
				fmt.Printf("Releasing lock...\n")
				err := c.LockRelease()
				if err != nil {
					fmt.Printf("Failed to release lock: %v\n", err)
				} else {
					fmt.Printf("Lock released\n")
				}

			case "append":
				fmt.Printf("Acquiring lock before file append...\n")
				err := c.AcquireLockWithRetry()
				if err != nil {
					fmt.Printf("Failed to acquire lock: %v\n", err)
					close(done)
					return
				}

				fmt.Printf("Appending to file %s...\n", *filename)
				err = c.FileAppend(*filename, []byte(*content))
				if err != nil {
					fmt.Printf("Failed to append to file: %v\n", err)
				} else {
					fmt.Printf("Successfully appended to file\n")
				}

				fmt.Printf("Releasing lock...\n")
				if err := c.LockRelease(); err != nil {
					fmt.Printf("Failed to release lock: %v\n", err)
				} else {
					fmt.Printf("Lock released\n")
				}

			case "hold":
				fmt.Printf("Acquiring lock and holding...\n")
				err := c.AcquireLockWithRetry()
				if err != nil {
					fmt.Printf("Failed to acquire lock: %v\n", err)
					close(done)
					return
				}
				fmt.Printf("Successfully acquired lock. Holding until timeout or interrupt...\n")
				// In "hold" mode, we just keep the lock until the program is terminated
				select {
				case <-timeoutCh:
					fmt.Printf("Timeout reached, releasing lock...\n")
					c.LockRelease()
					close(done)
					return
				}

			default:
				fmt.Printf("Unknown command: %s\n", command)
				close(done)
				return
			}

			// If it's not a hold command and we've done all repeats, we're done
			if command != "hold" && i == *repeat-1 {
				close(done)
				return
			}
		}
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		fmt.Printf("Operation completed\n")
	case <-timeoutCh:
		fmt.Printf("Operation timed out\n")
	}
}
