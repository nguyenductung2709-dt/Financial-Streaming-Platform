package main

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	for _, test := range []struct {
		name    string
		env     map[string]string
		want    config
		wantErr string
	}{
		{
			name: "defaults",
			want: config{databaseURL: "postgresql://localhost/test", brokers: []string{"localhost:19092"}, groupID: "materializer"},
		},
		{
			name: "overrides",
			env: map[string]string{
				"MATERIALIZER_DATABASE_URL": "postgresql://localhost/another",
				"MATERIALIZER_BROKERS":      " broker-a:9092, broker-b:9092 ",
				"MATERIALIZER_GROUP_ID":     "another-group",
				"MATERIALIZER_STAMP_TOPIC":  "materialize-stamps",
			},
			want: config{databaseURL: "postgresql://localhost/another", brokers: []string{"broker-a:9092", "broker-b:9092"}, groupID: "another-group", stampTopic: "materialize-stamps"},
		},
		{name: "missing database", env: map[string]string{"MATERIALIZER_DATABASE_URL": ""}, wantErr: "MATERIALIZER_DATABASE_URL"},
		{name: "blank database", env: map[string]string{"MATERIALIZER_DATABASE_URL": "  "}, wantErr: "MATERIALIZER_DATABASE_URL"},
		{name: "empty brokers", env: map[string]string{"MATERIALIZER_BROKERS": ""}, wantErr: "MATERIALIZER_BROKERS"},
		{name: "empty broker entry", env: map[string]string{"MATERIALIZER_BROKERS": "localhost:19092, "}, wantErr: "MATERIALIZER_BROKERS"},
		{name: "empty group", env: map[string]string{"MATERIALIZER_GROUP_ID": ""}, wantErr: "MATERIALIZER_GROUP_ID"},
		{name: "untrimmed group", env: map[string]string{"MATERIALIZER_GROUP_ID": " materializer"}, wantErr: "MATERIALIZER_GROUP_ID"},
		{name: "untrimmed stamp topic", env: map[string]string{"MATERIALIZER_STAMP_TOPIC": " stamps"}, wantErr: "MATERIALIZER_STAMP_TOPIC"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("MATERIALIZER_DATABASE_URL", "postgresql://localhost/test")
			for _, key := range []string{"MATERIALIZER_BROKERS", "MATERIALIZER_GROUP_ID", "MATERIALIZER_STAMP_TOPIC"} {
				t.Setenv(key, "") // Preserve the caller's environment for cleanup.
				if err := os.Unsetenv(key); err != nil {
					t.Fatal(err)
				}
			}
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			got, err := loadConfig()
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("loadConfig() error = %v; want %s", err, test.wantErr)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("loadConfig() = %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
}
