package validate

import "unicode"

// HasSpecialChar checks if the name contains special characters.
func HasSpecialChar(name string) bool {
	for _, r := range name {
		if r != '_' && !IsAlpha(r) && !IsNumber(r) {
			return true
		}
	}
	return false
}

// HasWhitespace checks if the string contains whitespace.
func HasWhitespace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}
	return false
}

// IsAlpha check if c is alpha.
func IsAlpha(c rune) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

// IsNumber check if c is a number.
func IsNumber(c rune) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}
