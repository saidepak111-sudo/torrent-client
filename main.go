package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <torrent file> <output file>\n", os.Args[0])
		os.Exit(1)
	}

	torrentPath := os.Args[1]
	outputPath := os.Args[2]

	tf, err := open(torrentPath)
	if err != nil {
		log.Fatalf("Could not open torrent file: %v\n", err)
	}

	data, err := tf.download()
	if err != nil {
		log.Fatalf("Download failed: %v\n", err)
	}

	err = os.WriteFile(outputPath, data, 0644)
	if err != nil {
		log.Fatalf("Could not write output file: %v\n", err)
	}

	fmt.Printf("Successfully downloaded %s to %s (%d bytes)\n", tf.name, outputPath, len(data))
}
