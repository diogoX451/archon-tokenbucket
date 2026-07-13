# archon-tokenbucket

[![Go Reference](https://pkg.go.dev/badge/github.com/diogoX451/archon-tokenbucket.svg)](https://pkg.go.dev/github.com/diogoX451/archon-tokenbucket)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Redis-backed **lazy-refill token bucket** for multi-tenant rate limiting.

Part of the **Archon open-source toolkit** by [@diogoX451](https://github.com/diogoX451).

## Install

```bash
go get github.com/diogoX451/archon-tokenbucket@latest
```

## Usage

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/diogoX451/archon-tokenbucket"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
lim := tokenbucket.NewTokenBucket(rdb)

dec, err := lim.TryConsume(ctx, "tenant-a", "api", tokenbucket.BucketConfig{
    Capacity: 100, RefillPerSecond: 10,
}, 1)
if !dec.Allowed {
    // use dec.RetryAfter
}
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0
