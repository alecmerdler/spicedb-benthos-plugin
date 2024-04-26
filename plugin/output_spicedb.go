package plugin

import (
	"context"
	"encoding/json"
	"fmt"

	authzedv1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/benthosdev/benthos/v4/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func spiceDBOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Update relationships in SpiceDB.").
		Description("TODO(alecmerdler): Write more detailed description.").Beta().
		Fields(
			service.NewURLField("endpoint").
				Description("The URL of the SpiceDB API endpoint.").
				Example("grpc.authzed.com:443"),
			service.NewStringField("token").
				Description(`SpiceDB API token.`).
				Example("my_token"),
			service.NewTLSField("tls"),
			service.NewIntField("max_in_flight").
				Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
				Default(64),
		)
}

func init() {
	err := service.RegisterOutput(
		"spicedb", spiceDBOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}

			writer, err := newSpiceDBWriterFromConfig(conf, mgr)
			if err != nil {
				return nil, 0, err
			}

			return writer, maxInFlight, err
		},
	)
	if err != nil {
		panic(err)
	}
}

type spicedbWriter struct {
	client *authzed.Client

	res    *service.Resources
	logger *service.Logger
}

func newSpiceDBWriterFromConfig(conf *service.ParsedConfig, res *service.Resources) (*spicedbWriter, error) {
	endpoint, err := conf.FieldURL("endpoint")
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint field: %w", err)
	}

	token, err := conf.FieldString("token")
	if err != nil {
		return nil, fmt.Errorf("failed to parse token field: %w", err)
	}

	// TODO(alecmerdler): Add TLS fields to the config and pass them when creating client...
	transportCredentialsOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	tokenOption := grpcutil.WithInsecureBearerToken(token)
	client, err := authzed.NewClient(endpoint.String(), tokenOption, transportCredentialsOption)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &spicedbWriter{
		client: client,
		res:    res,
		logger: res.Logger(),
	}, nil
}

func (s *spicedbWriter) Connect(_ context.Context) (err error) {
	return
}

func (s *spicedbWriter) Write(ctx context.Context, msg *service.Message) error {
	msgRaw, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("failed to convert message to bytes: %w", err)
	}

	// FIXME(alecmerdler): Debugging...
	s.logger.Info(fmt.Sprintf("Received message: %s", string(msgRaw)))

	var update authzedv1.RelationshipUpdate
	if err := json.Unmarshal(msgRaw, &update); err != nil {
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	_, err = s.client.WriteRelationships(ctx, &authzedv1.WriteRelationshipsRequest{
		// TODO(alecmerdler): Implement `WriteBatch()` to handle multiple updates at once...
		Updates: []*authzedv1.RelationshipUpdate{&update},
	})
	return fmt.Errorf("failed to write relationships: %w", err)
}

func (s *spicedbWriter) Close(_ context.Context) (err error) {
	return
}
