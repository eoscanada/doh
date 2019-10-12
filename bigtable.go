package main

import (
	"context"
	"os"
	"strings"

	"cloud.google.com/go/bigtable"
	"google.golang.org/api/option"
)

const emulatorHostDefault = "BIGTABLE_EMULATOR_HOST"
const emulatorDefaultHostValue = "localhost:8086"

func newBigTableClient(project, instance string, opts ...option.ClientOption) (*bigtable.Client, error) {
	ctx := context.Background()
	if isTestEnv(project, instance) && (os.Getenv(emulatorHostDefault) == "") {
		os.Setenv(emulatorHostDefault, emulatorDefaultHostValue)
	}

	client, err := bigtable.NewClient(ctx, project, instance, opts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func newBigTableAdminClient(project, instance string, opts ...option.ClientOption) (*bigtable.AdminClient, error) {
	ctx := context.Background()
	if isTestEnv(project, instance) && (os.Getenv(emulatorHostDefault) == "") {
		os.Setenv(emulatorHostDefault, emulatorDefaultHostValue)
	}

	client, err := bigtable.NewAdminClient(ctx, project, instance, opts...)
	if err != nil {
		return nil, err
	}
	return client, nil
}

func isTestEnv(project, instance string) bool {
	return (strings.HasPrefix(project, "dev") || strings.HasPrefix(instance, "dev"))
}
