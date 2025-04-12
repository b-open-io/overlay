module github.com/b-open-io/overlay

go 1.24.2

require (
	github.com/4chain-ag/go-overlay-services v0.0.0-00010101000000-000000000000
	github.com/bsv-blockchain/go-sdk v1.1.22
	github.com/joho/godotenv v1.5.1
	github.com/redis/go-redis/v9 v9.7.3
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/pkg/errors v0.9.1 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/exp v0.0.0-20220909182711-5c715a9e8561 // indirect
)

// replace github.com/4chain-ag/go-overlay-services => ../go-overlay-services

replace github.com/4chain-ag/go-overlay-services => github.com/4chain-ag/go-overlay-services v0.1.1-0.20250412185326-512e43a6d27c

replace github.com/bsv-blockchain/go-sdk => github.com/b-open-io/go-sdk v1.1.22-0.20250406003733-6a6b9ac5b847
