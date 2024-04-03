package plugin

import (
	"fmt"

	authzedv1 "github.com/authzed/authzed-go/v1"
	"github.com/benthosdev/benthos/v4/public/service"
)

var (
	sampleString = `{}`
)

type spicedbReadRelationshipsInput struct {
	client *authzedv1.Client
}

func newSpiceDBReadRelationshipsInputFromConfig(conf *service.ParsedConfig, res *service.Resources) (*spicedbReadRelationshipsInput, error) {
	// TODO(alecmerdler): Creacte a new SpiceDB client...
	input := &spicedbReadRelationshipsInput{}
	return input, nil
}

func spiceDBReadRelationshipsInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary(fmt.Sprintf("Uses the SpiceDB [ReadRelationships API](https://buf.build/authzed/api/docs/main:authzed.api.v1#authzed.api.v1.PermissionsService.ReadRelationships) and creates a message for each relationship received. Each message is a json object looking like: \n```json\n%s\n```", sampleString)).
		Description("TODO(alecmerdler: Write more detailed description.").
		Fields(
			service.NewStringField("token").
				Description(`SpiceDB API token.`).
				Example("my_token"),
			service.NewTLSField("tls"),
			service.NewURLField("endpoint").
				Description("The URL of the SpiceDB API endpoint.").
				Example("https://grpc.authzed.com:443"),
		)
}

func init() {
	err := service.RegisterInput(
		"spicedb", spiceDBReadRelationshipsInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSpiceDBReadRelationshipsInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})

	if err != nil {
		panic(err)
	}
}
