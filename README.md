[![Go Report Card](https://goreportcard.com/badge/github.com/aergoio/aergo-lib)](https://goreportcard.com/report/github.com/aergoio/aergo-lib)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Travis_ci](https://travis-ci.org/aergoio/aergo-lib.svg?branch=master)](https://travis-ci.org/aergoio/aergo-lib)
[![Maintainability](https://api.codeclimate.com/v1/badges/a055db179465dc8176f4/maintainability)](https://codeclimate.com/github/aergoio/aergo-lib/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/a055db179465dc8176f4/test_coverage)](https://codeclimate.com/github/aergoio/aergo-lib/test_coverage)
[![API Reference](https://godoc.org/github.com/aergoio/aergo-lib?status.svg)](https://godoc.org/github.com/aergoio/aergo-lib)

# aergo-lib

This repository is a collection of common libraries used in the aergo project.
See [godoc](https://godoc.org/github.com/aergoio/aergo-lib) to get more detail descriptions and usages.

## config

Package config provides an easy way to create and manage configurations for aergo projects written in go.

## db

Package db is an wrapper of database implementations. Currently, this supports [badgerdb](https://github.com/dgraph-io/badger).
More implementations (e.g. leveldb) will be updated in the future

## log

Package log is a global and configurable logger pkg, based on [zerolog](https://github.com/rs/zerolog)