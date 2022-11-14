package utils

import (
	"fmt"
	"log"
	"testing"

	"github.com/sony/sonyflake"
)

func TestSonyflake(t *testing.T) {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	// Note: this is base16, could shorten by encoding as base62 string
	fmt.Printf("github.com/sony/sonyflake:      %d\n", id)
}
