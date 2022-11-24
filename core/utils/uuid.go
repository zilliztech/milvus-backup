package utils

import (
	"log"

	"github.com/google/uuid"
)

func UUID() string {
	// 基于时间
	u, err := uuid.NewUUID()
	if err != nil {
		log.Fatal(err)
	}
	return u.String()
}
