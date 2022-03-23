package client

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func Connect(ctx context.Context, uri string, auth neo4j.AuthToken, config Neo4jClientConfiguration) (Neo4jClient, error) {
	driver, err := neo4j.NewDriverWithContext(uri, auth, config.AsDriverConfigurers()...)
	if err != nil {
		return nil, err
	}
	return Wrap(ctx, driver, config)
}

func Wrap(ctx context.Context, driver neo4j.DriverWithContext, config Neo4jClientConfiguration) (Neo4jClient, error) {
	if err := driver.VerifyConnectivity(ctx); err != nil {
		return nil, err
	}
	return &neo4jClient{
		driver:  driver,
		session: driver.NewSession(config.AsSessionConfig()),
	}, nil
}

type Neo4jClient interface {
	Run(ctx context.Context, statement CypherStatement, mapper ResultMapper) (any, error)
	BeginTransaction(ctx context.Context) (Neo4jTransaction, error)
	Close(ctx context.Context) error
}

type Neo4jClientConfiguration struct {
}

func (*Neo4jClientConfiguration) AsDriverConfigurers() []func(config *neo4j.Config) {
	return nil
}

func (*Neo4jClientConfiguration) AsSessionConfig() neo4j.SessionConfig {
	return neo4j.SessionConfig{}
}

type CypherQuery struct {
	Query      string
	Parameters map[string]any
}

type CypherStatement struct {
	Query      string
	Parameters map[string]any
	Autocommit bool
}

type ResultMapper func(ctx context.Context, result neo4j.ResultWithContext) (any, error)

type Neo4jTransaction interface {
	Run(ctx context.Context, query CypherQuery) (neo4j.ResultWithContext, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type neo4jTransaction struct {
	transaction neo4j.TransactionWithContext
}

func (n *neo4jTransaction) Run(ctx context.Context, query CypherQuery) (neo4j.ResultWithContext, error) {
	return n.transaction.Run(ctx, query.Query, query.Parameters)
}

// TODO: grab bookmarks
func (n *neo4jTransaction) Commit(ctx context.Context) error {
	return n.transaction.Commit(ctx)
}

func (n *neo4jTransaction) Rollback(ctx context.Context) error {
	return n.transaction.Rollback(ctx)
}

type neo4jClient struct {
	driver  neo4j.DriverWithContext
	session neo4j.SessionWithContext
}

func (client *neo4jClient) Run(ctx context.Context, statement CypherStatement, mapper ResultMapper) (_ any, err error) {
	if statement.Autocommit {
		neo4jResult, err := client.session.Run(ctx, statement.Query, statement.Parameters)
		if err != nil {
			return nil, err
		}
		result, err := mapper(ctx, neo4jResult)
		if err != nil {
			return nil, err
		}
		return result, err
	}
	transaction, err := client.session.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		txErr := transaction.Close(ctx)
		if err == nil {
			err = txErr
			return
		}
		err = fmt.Errorf("tx could not close: %v, but error occurred first: %w", txErr, err)
	}()
	neo4jResult, err := transaction.Run(ctx, statement.Query, statement.Parameters)
	if err != nil {
		return nil, err
	}
	result, err := mapper(ctx, neo4jResult)
	if err != nil {
		return nil, err
	}
	return result, transaction.Commit(ctx)
}

// TODO: open new session to allow multiple TX to run, grab bookmarks from existing session
func (client *neo4jClient) BeginTransaction(ctx context.Context) (Neo4jTransaction, error) {
	tx, err := client.session.BeginTransaction(ctx)
	if err != nil {
		return nil, err
	}
	return &neo4jTransaction{transaction: tx}, nil
}

func (client *neo4jClient) Close(ctx context.Context) error {
	sessionErr := client.session.Close(ctx)
	driverErr := client.driver.Close(ctx)
	switch {
	case sessionErr != nil && driverErr != nil:
		return fmt.Errorf("session closure error: %v and driver closure error: %w", sessionErr, driverErr)
	case sessionErr != nil:
		return sessionErr
	case driverErr != nil:
		return driverErr
	default:
		return nil
	}
}
