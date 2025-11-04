[![Go Report Card](https://goreportcard.com/badge/github.com/aergoio/aergo-lib)](https://goreportcard.com/report/github.com/aergoio/aergo-lib)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Maintainability](https://api.codeclimate.com/v1/badges/a055db179465dc8176f4/maintainability)](https://codeclimate.com/github/aergoio/aergo-lib/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a055db179465dc8176f4/test_coverage)](https://codeclimate.com/github/aergoio/aergo-lib/test_coverage)
[![API Reference](https://godoc.org/github.com/aergoio/aergo-lib?status.svg)](https://godoc.org/github.com/aergoio/aergo-lib)

[comment]: <> (CI badge will be added on next release. )

# aergo-lib

This repository is a collection of common libraries used in the aergo project.
See [godoc](https://godoc.org/github.com/aergoio/aergo-lib) to get more detail descriptions and usages.

## config

Package config provides an easy way to create and manage configurations for aergo projects written in go.

## db

Package db is a wrapper of database implementations. Currently, this supports:
- [BadgerDB](https://github.com/dgraph-io/badger)
- [LevelDB](https://github.com/syndtr/goleveldb)
- [RocksDB](https://rocksdb.org/)

### RocksDB Requirements

RocksDB requires CGO and the RocksDB C++ library to be installed on your system.

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install -y librocksdb-dev
```

**Alpine Linux:**
```bash
apk add --no-cache rocksdb-dev build-base
```

**RedHat/CentOS/Fedora:**
```bash
sudo yum install -y rocksdb-devel
# or on newer versions
sudo dnf install -y rocksdb-devel
```

**macOS:**
```bash
brew install rocksdb
```

**Building with RocksDB:**
```bash
CGO_ENABLED=1 go build ./...
```

## log

Package log is a global and configurable logger pkg, based on [zerolog](https://github.com/rs/zerolog)