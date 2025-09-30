package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/urfave/cli/v3"
)

var app = cli.Command{
	Name: "run",
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:  "nats-hosts",
			Value: []string{"host.docker.internal:4222"},
		},
	},
	Action: runCommand,
}

func main() {
	ctx := context.Background()
	err := app.Run(ctx, os.Args)
	if err != nil {
		slog.Error("error with executing: " + err.Error())
	}
}

func runCommand(ctx context.Context, command *cli.Command) error {
	natsHosts := strings.Join(command.StringSlice("nats-hosts"), ",")

	nc, err := nats.Connect(natsHosts, nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
		if err != nil {
			slog.Error("NATS was disconnected: " + err.Error())
			os.Exit(1)
		}
	}))
	if err != nil {
		return errors.New("error with nats connection: " + err.Error())
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return errors.New("error with use jetstream: " + err.Error())
	}

	objectStore, err := js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:   "test_bucket",
		Storage:  jetstream.FileStorage,
		Replicas: 3,
		Placement: &jetstream.Placement{
			Cluster: "c1",
		},
	})
	if err != nil {
		return errors.New("error with create object store: " + err.Error())
	}

	objInfo, err := objectStore.PutString(ctx, "test", "TEST_STRING_LOL")
	if err != nil {
		return errors.New("error with add file: " + err.Error())
	}

	_, err = objectStore.AddLink(ctx, "test32323", objInfo)
	if err != nil {
		panic(err)
	}

	objectInfos, err := objectStore.List(ctx)
	if err != nil {
		panic(err)
	}

	for _, info := range objectInfos {
		slog.Info(fmt.Sprintf("OBJECT: %+v", info))
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// DOWNLOAD
		objectName := r.URL.Query().Get("object_name")

		info, err := objectStore.GetInfo(ctx, objectName)
		if err != nil {
			slog.Error("error with getting object: " + err.Error())
			return
		}

		w.Header().Set("Last-Modified", info.ModTime.Format(time.RFC850))
		w.Header().Set("ETag", info.NUID)

		// FIRSTLY CHECK ETAG
		// IF IT MISSING -> CHECK LAST MODIFIED

		ifNoneMatch := r.Header.Get("If-None-Match")

		var ifModifiedSince = new(time.Time)
		*ifModifiedSince, err = time.Parse(r.Header.Get("If-Modified-Since"), time.RFC850)
		if err != nil {
			slog.Error("error with parsing if modified since: " + err.Error())
			ifModifiedSince = nil
		}

		if ifModifiedSince != nil && info.ModTime.Before(*ifModifiedSince) {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		if info.NUID == ifNoneMatch {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		result, err := objectStore.Get(ctx, objectName)
		if err != nil {
			slog.Error("error with getting object: " + err.Error())
			return
		}

		w.Header().Set("Cache-Control", "max-age=10")

		written, err := io.Copy(w, result)
		if err != nil {
			panic(err)
		}

		slog.Info("", slog.Attr{Key: "written", Value: slog.AnyValue(written)})
	})

	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		// UPLOAD
		objectName := r.URL.Query().Get("object_name")

		result, err := objectStore.Put(ctx, jetstream.ObjectMeta{
			Name: objectName,
		}, r.Body)
		if err != nil {
			slog.Error("error with uploading object: " + err.Error())
			return
		}

		slog.Info(fmt.Sprintf("success: %+v", result))
	})

	slog.Info("start listening")

	return http.ListenAndServe(":8080", nil)
}
