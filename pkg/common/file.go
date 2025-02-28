package common

import "strings"

func SplitPath(path string) []string {
	// Remove leading/trailing slashes and split the path
	trimmed := strings.Trim(path, "/")

	if trimmed == "" {
		return []string{}
	}

	return strings.Split(trimmed, "/")
}
