package main

import (
	"context"
	"fmt"
	"github.com/fbiville/neo4j-go-client/pkg/client"
	"github.com/fbiville/neo4j-go-client/pkg/errors"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"time"
)

func main() {
	parentCtx := context.Background()
	ctx, cancelFunc := context.WithTimeout(parentCtx, 30*time.Second)
	defer cancelFunc()

	container, driver, err := StartNeo4jContainer(ctx, ContainerConfiguration{
		Neo4jVersion: "4.4",
		Username:     "neo4j",
		Password:     "foobar",
	})
	neo4jClient, err := client.Wrap(ctx, driver, client.Neo4jClientConfiguration{})
	if err != nil {
		panic(err)
	}
	defer close(ctx, container, neo4jClient)

	runSimpleQuery(ctx, neo4jClient)
	runAutocommitQuery(ctx, neo4jClient)
	runExplicitTx(ctx, neo4jClient)
	runExplicitTxWithCustomRetry(ctx, neo4jClient)

}

func runSimpleQuery(ctx context.Context, neo4jClient client.Neo4jClient) {
	answer, err := neo4jClient.Run(ctx, client.CypherStatement{Query: "RETURN 42 AS fortyTwo"}, fortyTwo)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Simple query: %d\n", answer)
}

func runAutocommitQuery(ctx context.Context, neo4jClient client.Neo4jClient) {
	answer, err := neo4jClient.Run(ctx, client.CypherStatement{Query: `CALL { RETURN 42 } IN TRANSACTIONS RETURN 42 AS fortyTwo`, Autocommit: true}, fortyTwo)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Autocommit query: %d\n", answer)
}

func runExplicitTx(ctx context.Context, neo4jClient client.Neo4jClient) {
	transaction, err := neo4jClient.BeginTransaction(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := transaction.Commit(ctx); err != nil {
			panic(err)
		}
	}()
	result, err := transaction.Run(ctx, client.CypherQuery{
		Query:      "RETURN $value AS fortyTwo",
		Parameters: map[string]any{"value": 42},
	})
	if err != nil {
		panic(err)
	}
	answer, err := fortyTwo(ctx, result)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Explicit TX query: %d\n", answer)
}

func runExplicitTxWithCustomRetry(ctx context.Context, neo4jClient client.Neo4jClient) {
	retryTransaction, err := neo4jClient.BeginTransaction(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := retryTransaction.Commit(ctx); err != nil {
			panic(err)
		}
	}()

	// custom retry
	numberOfRetries := 5
	for numberOfRetries > 0 {
		numberOfRetries--
		result, err := retryTransaction.Run(ctx, client.CypherQuery{
			Query:      "RETURN $value AS fortyTwo",
			Parameters: map[string]any{"value": 42},
		})
		if err != nil {
			if errors.IsRetryable(err) {
				time.Sleep(time.Duration(6-numberOfRetries) * time.Second)
				continue
			}
			panic(err)
		}
		answer, err := fortyTwo(ctx, result)
		if err != nil {
			if errors.IsRetryable(err) {
				time.Sleep(time.Duration(6-numberOfRetries) * time.Second)
				continue
			}
			panic(err)
		}
		fmt.Printf("Explicit retryable TX query: %d\n", answer)
		break
	}
}

func fortyTwo(ctx context.Context, queryResult neo4j.ResultWithContext) (any, error) {
	record, err := queryResult.Single(ctx)
	if err != nil {
		return nil, err
	}
	result, _ := record.Get("fortyTwo")
	return result.(int64), nil
}

func close(ctx context.Context, container testcontainers.Container, client client.Neo4jClient) {
	if client != nil {
		_ = client.Close(ctx)
	}
	if container != nil {
		_ = container.Terminate(ctx)
	}
}
