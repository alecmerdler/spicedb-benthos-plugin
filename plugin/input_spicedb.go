package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	authzedv1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/authzed/authzed-go/v1"
	"github.com/authzed/grpcutil"
	"github.com/benthosdev/benthos/v4/public/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type consistencyOption string

const (
	consistencyOptionFull            consistencyOption = "Full"
	consistencyOptionAtExactSnapshot consistencyOption = "AtExactSnapshot"
	consistencyOptionAtLeastAsFresh  consistencyOption = "AtLeastAsFresh"
	consistencyOptionMinimizeLatency consistencyOption = "MinimizeLatency"
)

var (
	sampleString = `{}`
)

type spiceDBInput struct {
	consistency *authzedv1.Consistency
	limit       uint32
	filter      *authzedv1.RelationshipFilter

	client              *authzed.Client
	dbMut               sync.Mutex
	relationshipsClient authzedv1.PermissionsService_ReadRelationshipsClient

	// TODO(alecmerdler): Store the cursor returned by the API on disk for resuming after restarts...

	res    *service.Resources
	logger *service.Logger
}

func newSpiceDBInputFromConfig(conf *service.ParsedConfig, res *service.Resources) (*spiceDBInput, error) {
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

	zedToken, _ := conf.FieldString("zedtoken")
	consistencyValue, err := conf.FieldString("consistency")
	if err != nil {
		return nil, fmt.Errorf("failed to parse consistency field: %w", err)
	}

	consistency, err := consistencyFrom(consistencyValue, zedToken)
	if err != nil {
		return nil, fmt.Errorf("failed to determine consistency: %w", err)
	}

	var limit uint32
	if configLimit, err := conf.FieldInt("limit"); err == nil {
		if configLimit < 1 {
			return nil, fmt.Errorf("limit must be greater than 0")
		}

		limit = uint32(configLimit)
	}

	filterConfig := conf.Namespace("filter")
	resourceType, err := filterConfig.FieldString("resource_type")
	if err != nil {
		return nil, fmt.Errorf("failed to parse resource_type field: %w", err)
	}

	resourceID, _ := filterConfig.FieldString("resource_id")
	relation, _ := filterConfig.FieldString("relation")
	subjectType, _ := filterConfig.FieldString("subject_type")
	subjectID, _ := filterConfig.FieldString("subject_id")
	subjectRelation, _ := filterConfig.FieldString("subject_relation")

	filter := &authzedv1.RelationshipFilter{
		ResourceType:       resourceType,
		OptionalResourceId: resourceID,
		OptionalRelation:   relation,
	}

	if subjectType != "" {
		filter.OptionalSubjectFilter = &authzedv1.SubjectFilter{
			SubjectType:       subjectType,
			OptionalSubjectId: subjectID,
		}

		if subjectRelation != "" {
			filter.OptionalSubjectFilter.OptionalRelation = &authzedv1.SubjectFilter_RelationFilter{
				Relation: subjectRelation,
			}
		}
	}

	return &spiceDBInput{
		consistency: consistency,
		limit:       limit,
		filter:      filter,
		client:      client,
		res:         res,
		logger:      res.Logger(),
	}, nil
}

func spiceDBInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Integration").
		Summary(fmt.Sprintf("Uses the SpiceDB [ReadRelationships API](https://buf.build/authzed/api/docs/main:authzed.api.v1#authzed.api.v1.PermissionsService.ReadRelationships) and creates a message for each relationship received. Each message is a json object looking like: \n```json\n%s\n```", sampleString)).
		Description("TODO(alecmerdler): Write more detailed description.").
		Fields(
			service.NewURLField("endpoint").
				Description("The URL of the SpiceDB API endpoint.").
				Example("grpc.authzed.com:443"),
			service.NewStringField("token").
				Description(`SpiceDB API token.`).
				Example("my_token"),
			service.NewTLSField("tls"),
			// FIXME(alecmerdler): Use `service.NewObjectField()` to group the consistency type and zedtoken fields under `consistency`...
			service.NewStringAnnotatedEnumField("consistency", map[string]string{
				string(consistencyOptionFull):            "All data used in the API call *must* be at the most recent snapshot found.",
				string(consistencyOptionAtExactSnapshot): "All data used in the API call must be *at the given* snapshot in time; if the snapshot is no longer available, an error will be returned to the caller.",
				string(consistencyOptionAtLeastAsFresh):  "All data used in the API call must be *at least as fresh* as that found in the ZedToken; more recent data might be used if available or faster.",
				string(consistencyOptionMinimizeLatency): "The latency for the call should be minimized by having the system select the fastest snapshot available.",
			}).
				Default(string(consistencyOptionMinimizeLatency)).
				Advanced(),
			service.NewStringField("zedtoken").
				Description("The ZedToken to use for the AtExactSnapshot or AtLeastAsFresh consistency modes.").
				Optional().
				Advanced(),
			service.NewIntField("limit").
				Optional().
				Description("The maximum number of relationships to read.").
				Advanced(),
			service.NewObjectField("filter",
				service.NewStringField("resource_type"),
				service.NewStringField("resource_id").
					Optional(),
				service.NewStringField("relation").
					Optional(),
				service.NewStringField("subject_type").
					Optional(),
				service.NewStringField("subject_id").
					Optional(),
				service.NewStringField("subject_relation").
					Optional(),
			).
				Description("Only relationships matching this filter will be yielded."),
		)
}

func init() {
	err := service.RegisterInput(
		"spicedb", spiceDBInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSpiceDBInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})

	if err != nil {
		panic(err)
	}
}

func (i *spiceDBInput) Connect(ctx context.Context) (err error) {
	i.dbMut.Lock()
	defer i.dbMut.Unlock()

	if i.relationshipsClient != nil {
		return fmt.Errorf("already connected")
	}

	relationshipsClient, err := i.client.ReadRelationships(ctx, &authzedv1.ReadRelationshipsRequest{
		Consistency:        i.consistency,
		RelationshipFilter: i.filter,
		OptionalLimit:      i.limit,
	})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	i.relationshipsClient = relationshipsClient
	return
}

func (i *spiceDBInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	i.dbMut.Lock()
	relationshipsClient := i.relationshipsClient
	i.dbMut.Unlock()

	if relationshipsClient == nil {
		return nil, nil, fmt.Errorf("not connected")
	}

	res, err := relationshipsClient.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			err = service.ErrEndOfInput
		}

		_ = relationshipsClient.CloseSend()
		return nil, nil, err
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal response to JSON: %w", err)
	}

	msg := service.NewMessage(jsonBytes)
	return msg, func(ctx context.Context, err error) (cErr error) {
		// TODO(alecmerdler): Persist `res.AfterResultCursor` to disk for resuming after restarts...
		return
	}, nil
}

func (i *spiceDBInput) Close(ctx context.Context) (err error) {
	i.dbMut.Lock()
	defer i.dbMut.Unlock()

	select {
	case <-i.relationshipsClient.Context().Done():
		// Already closed.
	default:
		if err := i.relationshipsClient.CloseSend(); err != nil {
			return fmt.Errorf("failed to close client: %w", err)
		}
	}

	return
}

func consistencyFrom(configValue string, zedToken string) (*authzedv1.Consistency, error) {
	consistency := consistencyOption(configValue)
	switch consistency {
	case consistencyOptionFull:
		return &authzedv1.Consistency{
			Requirement: &authzedv1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		}, nil
	case consistencyOptionAtExactSnapshot:
		if zedToken == "" {
			return nil, fmt.Errorf("zedtoken must be provided when using AtExactSnapshot consistency")
		}

		return &authzedv1.Consistency{
			Requirement: &authzedv1.Consistency_AtExactSnapshot{
				AtExactSnapshot: &authzedv1.ZedToken{Token: zedToken},
			},
		}, nil
	case consistencyOptionAtLeastAsFresh:
		if zedToken == "" {
			return nil, fmt.Errorf("zedtoken must be provided when using AtLeastAsFresh consistency")
		}

		return &authzedv1.Consistency{
			Requirement: &authzedv1.Consistency_AtLeastAsFresh{
				AtLeastAsFresh: &authzedv1.ZedToken{Token: zedToken},
			},
		}, nil
	case consistencyOptionMinimizeLatency:
		fallthrough
	default:
		return &authzedv1.Consistency{
			Requirement: &authzedv1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		}, nil
	}
}
