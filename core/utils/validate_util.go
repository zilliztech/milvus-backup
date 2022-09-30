package utils

import (
	"errors"
	"fmt"
	"strings"
)

const (
	MaxNameLength = 128
)

func ValidateType(entity, entityType string) error {
	entity = strings.TrimSpace(entity)

	if entity == "" {
		return fmt.Errorf("collection %s should not be empty", entityType)
	}

	invalidMsg := fmt.Sprintf("Invalid collection %s: %s. ", entityType, entity)
	if int64(len(entity)) > MaxNameLength {
		msg := invalidMsg + fmt.Sprintf("The length of %s must be less than %d characters.", entityType, MaxNameLength)
		return errors.New(msg)
	}

	firstChar := entity[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + fmt.Sprintf("The first character of  %s must be an underscore or letter.", entityType)
		return errors.New(msg)
	}

	for i := 1; i < len(entity); i++ {
		c := entity[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + fmt.Sprintf("%s can only contain numbers, letters and underscores.", entityType)
			return errors.New(msg)
		}
	}
	return nil
}

// isAlpha check if c is alpha.
func isAlpha(c uint8) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

// isNumber check if c is a number.
func isNumber(c uint8) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}
