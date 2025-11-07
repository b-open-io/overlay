package utils

import (
	"net/url"
	"strings"
)

// SanitizeConnectionString removes credentials from connection strings for safe logging
func SanitizeConnectionString(connStr string) string {
	if connStr == "" {
		return ""
	}

	// Handle redis:// URLs
	if strings.HasPrefix(connStr, "redis://") || strings.HasPrefix(connStr, "rediss://") {
		parsedURL, err := url.Parse(connStr)
		if err != nil {
			// If parsing fails, just redact the whole thing after the scheme
			parts := strings.SplitN(connStr, "://", 2)
			if len(parts) == 2 {
				return parts[0] + "://*****"
			}
			return "*****"
		}

		// Redact password
		if parsedURL.User != nil {
			parsedURL.User = url.UserPassword(parsedURL.User.Username(), "*****")
		}

		return parsedURL.String()
	}

	// Handle mongodb:// and mongodb+srv:// URLs
	if strings.HasPrefix(connStr, "mongodb://") || strings.HasPrefix(connStr, "mongodb+srv://") {
		parsedURL, err := url.Parse(connStr)
		if err != nil {
			parts := strings.SplitN(connStr, "://", 2)
			if len(parts) == 2 {
				return parts[0] + "://*****"
			}
			return "*****"
		}

		// Redact password
		if parsedURL.User != nil {
			parsedURL.User = url.UserPassword(parsedURL.User.Username(), "*****")
		}

		return parsedURL.String()
	}

	// For unknown formats, try to redact anything that looks like a password
	// Pattern: anything between : and @ in a URL-like string
	if strings.Contains(connStr, "@") {
		parts := strings.Split(connStr, "@")
		if len(parts) >= 2 {
			userPart := parts[0]
			if colonIdx := strings.LastIndex(userPart, ":"); colonIdx != -1 {
				return userPart[:colonIdx+1] + "*****@" + strings.Join(parts[1:], "@")
			}
		}
	}

	return connStr
}
