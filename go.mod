module github.com/aergoio/aergo-lib

go 1.13

require (
	github.com/dgraph-io/badger/v3 v3.2104.3
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13 // indirect
	github.com/fsnotify/fsnotify v1.4.8-0.20180830220226-ccc981bf8038 // indirect
	github.com/gin-gonic/gin v1.7.4
	github.com/guptarohit/asciigraph v0.4.1
	github.com/hashicorp/hcl v1.0.1-0.20180906183839-65a6292f0157 // indirect
	github.com/mattn/go-colorable v0.1.4
	github.com/onsi/ginkgo v1.10.3 // indirect
	github.com/onsi/gomega v1.7.1 // indirect
	github.com/pelletier/go-toml v1.2.1-0.20180930205832-81a861c69d25 // indirect
	github.com/rs/zerolog v1.22.0
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.5.0
	github.com/stretchr/testify v1.4.0
	github.com/syndtr/goleveldb v1.0.0
)

replace github.com/dgraph-io/badger/v3 => github.com/shepelt/badger/v3 v3.2104.4
