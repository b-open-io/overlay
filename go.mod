module github.com/b-open-io/overlay

go 1.24.2

require (
	github.com/4chain-ag/go-overlay-services v0.0.0-00010101000000-000000000000
	github.com/bsv-blockchain/go-sdk v1.1.22
	github.com/joho/godotenv v1.5.1
	github.com/redis/go-redis/v9 v9.7.3
	go.mongodb.org/mongo-driver/v2 v2.2.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.1.2 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/exp v0.0.0-20220909182711-5c715a9e8561 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/text v0.22.0 // indirect
)

// replace github.com/4chain-ag/go-overlay-services => ../go-overlay-services

replace github.com/4chain-ag/go-overlay-services => github.com/4chain-ag/go-overlay-services v0.1.1-0.20250415204231-5c9e736aceec

replace github.com/bsv-blockchain/go-sdk => github.com/b-open-io/go-sdk v1.1.22-0.20250406003733-6a6b9ac5b847
