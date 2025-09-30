package main

import (
	"fmt"
	"log/slog"

	"github.com/go-resty/resty/v2"

	_ "embed"
)

//go:embed water.jpg
var body []byte

const objectName = "water2"

func main() {
	client := resty.New()

	resp, err := client.R().SetBody(body).Put("http://localhost:8080/upload?object_name=" + objectName)
	if err != nil {
		panic(err)
	}

	slog.Info(fmt.Sprintf("response: %v", resp))
}

// TODO:
// 1. cache control (etag, mod time)
// 2. access management
// 3. management api (upload, open/close bucket)
