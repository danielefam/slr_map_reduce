// Command make_hosts fetches available hosts and writes them to a file.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

// Implements a CLI command with the following flags:
//
//	-n 10        number of hosts to save to the file
//	-f hosts.txt path to the file
func main() {
	n := flag.Int("n", 10, "number of hosts to save to the file")
	f := flag.String("f", "hosts.txt", "path to the file")
	flag.Parse()

	hosts, err := getAvailableHosts()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error fetching hosts: %v\n", err)
		os.Exit(1)
	}

	if *n < len(hosts) {
		hosts = hosts[:*n]
	}

	content := strings.Join(hosts, "\n") + "\n"
	if err := os.WriteFile(*f, []byte(content), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Wrote %d hosts to %s\n", len(hosts), *f)
}

// getAvailableHosts sends a request to https://tp.telecom-paris.fr/ajax.php
// and parses the response into a list of names of all available machines.
func getAvailableHosts() ([]string, error) {
	resp, err := http.Get("https://tp.telecom-paris.fr/ajax.php")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result struct {
		Data [][]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	var hosts []string
	for _, entry := range result.Data {
		if len(entry) < 2 {
			continue
		}
		var name string
		if err := json.Unmarshal(entry[0], &name); err != nil {
			continue
		}
		var available bool
		if err := json.Unmarshal(entry[1], &available); err != nil {
			continue
		}
		if available {
			hosts = append(hosts, name+".enst.fr")
		}
	}

	return hosts, nil
}
