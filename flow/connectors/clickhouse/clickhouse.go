package connclickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	metadataStore "github.com/PeerDB-io/peer-flow/connectors/external_metadata"
	conns3 "github.com/PeerDB-io/peer-flow/connectors/s3"
	"github.com/PeerDB-io/peer-flow/connectors/utils"
	"github.com/PeerDB-io/peer-flow/generated/protos"
	"github.com/PeerDB-io/peer-flow/shared"
)

type ClickhouseConnector struct {
	ctx                context.Context
	database           *sql.DB
	pgMetadata         *metadataStore.PostgresMetadataStore
	tableSchemaMapping map[string]*protos.TableSchema
	logger             slog.Logger
	config             *protos.ClickhouseConfig
	creds              utils.S3PeerCredentials
}

func ValidateS3(ctx context.Context, bucketUrl string, creds utils.S3PeerCredentials) error {
	// for validation purposes
	s3Client, err := utils.CreateS3Client(creds)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	validErr := conns3.ValidCheck(ctx, s3Client, bucketUrl, nil)
	if validErr != nil {
		return validErr
	}

	return nil
}

func NewClickhouseConnector(ctx context.Context,
	config *protos.ClickhouseConfig,
) (*ClickhouseConnector, error) {
	database, err := connect(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	pgMetadata, err := metadataStore.NewPostgresMetadataStore(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "failed to create postgres metadata store", slog.Any("error", err))
		return nil, err
	}

	s3PeerCreds := utils.S3PeerCredentials{
		AccessKeyID:     config.AccessKeyId,
		SecretAccessKey: config.SecretAccessKey,
		Region:          config.Region,
	}

	validateErr := ValidateS3(ctx, config.S3Path, s3PeerCreds)
	if validateErr != nil {
		return nil, fmt.Errorf("failed to validate S3 bucket: %w", validateErr)
	}

	flowName, _ := ctx.Value(shared.FlowNameKey).(string)
	return &ClickhouseConnector{
		ctx:                ctx,
		database:           database,
		pgMetadata:         pgMetadata,
		tableSchemaMapping: nil,
		logger:             *slog.With(slog.String(string(shared.FlowNameKey), flowName)),
		config:             config,
		creds:              s3PeerCreds,
	}, nil
}

func connect(ctx context.Context, config *protos.ClickhouseConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("tcp://%s:%d?username=%s&password=%s", // TODO &database=%s"
		config.Host, config.Port, config.User, config.Password) // TODO , config.Database

	conn, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection to Clickhouse peer: %w", err)
	}

	if err := conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping to Clickhouse peer: %w", err)
	}

	// Execute USE database command to select a specific database
	_, err = conn.Exec(fmt.Sprintf("USE %s", config.Database))
	if err != nil {
		return nil, fmt.Errorf("failed in selecting db in Clickhouse peer: %w", err)
	}

	return conn, nil
}

func (c *ClickhouseConnector) Close() error {
	if c == nil || c.database == nil {
		return nil
	}

	err := c.database.Close()
	if err != nil {
		return fmt.Errorf("error while closing connection to Clickhouse peer: %w", err)
	}
	return nil
}

func (c *ClickhouseConnector) ConnectionActive() error {
	if c == nil || c.database == nil {
		return fmt.Errorf("ClickhouseConnector is nil")
	}

	// This also checks if database exists
	err := c.database.PingContext(c.ctx)
	return err
}