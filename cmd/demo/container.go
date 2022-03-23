package main

import (
	"context"
	"fmt"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type ContainerConfiguration struct {
	Neo4jVersion string
	Username     string
	Password     string
}

func (config ContainerConfiguration) neo4jAuthEnvVar() string {
	return fmt.Sprintf("%s/%s", config.Username, config.Password)
}

func (config ContainerConfiguration) neo4jAuthToken() neo4j.AuthToken {
	return neo4j.BasicAuth(config.Username, config.Password, "")
}

func StartNeo4jContainer(ctx context.Context, config ContainerConfiguration) (testcontainers.Container, neo4j.DriverWithContext, error) {
	request := testcontainers.ContainerRequest{
		Image:        "neo4j:4.4",
		ExposedPorts: []string{"7687/tcp"},
		Env:          map[string]string{"NEO4J_AUTH": config.neo4jAuthEnvVar()},
		WaitingFor:   wait.ForLog("Bolt enabled"),
	}
	container, err := testcontainers.GenericContainer(ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: request,
			Started:          true,
		})
	if err != nil {
		return nil, nil, err
	}
	driver, err := newNeo4jDriver(ctx, container, config.neo4jAuthToken())
	return container, driver, err
}

func newNeo4jDriver(ctx context.Context, container testcontainers.Container, auth neo4j.AuthToken) (neo4j.DriverWithContext, error) {
	port, err := container.MappedPort(ctx, "7687")
	if err != nil {
		return nil, err
	}
	uri := fmt.Sprintf("neo4j://localhost:%d", port.Int())
	return neo4j.NewDriverWithContext(uri, auth)
}
