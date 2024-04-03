package main

import (
	"context"

	_ "github.com/benthosdev/benthos/v4/public/components/all"

	_ "github.com/alecmerdler/spicedb-benthos-plugin/v2/plugin"
	"github.com/benthosdev/benthos/v4/public/service"
)

func main() {
	service.RunCLI(context.Background())
}
