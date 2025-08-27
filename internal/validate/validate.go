package validate

func HasSpecialChar(name string) bool {
	for i := 1; i < len(name); i++ {
		c := name[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			return true
		}
	}
	return false
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
