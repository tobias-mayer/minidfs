package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

type Chunkserver struct {
	Master string
	Port   string
	Dir    string
}

func NewChunkserver(master string, port string, dir string) *Chunkserver {
	return &Chunkserver{master, port, dir}
}

func (chunkserver *Chunkserver) run() error {
	registerResponse, err := chunkserver.registerAtMaster()
	if err != nil {
		log.Fatalf("registering at master failed: %v", err)
	}

	log.Print(registerResponse)

	chunkserver.registerUploadChunk()
	chunkserver.registerGetChunkEndpoint()

	if err := http.ListenAndServe(":"+chunkserver.Port, nil); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (chunkserver *Chunkserver) registerUploadChunk() {
	http.HandleFunc("/uploadChunk", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, fmt.Sprintf("error parsing multipart form: %v", err), http.StatusBadRequest)
			return
		}

		file, fileHeader, err := r.FormFile("chunk")
		if err != nil {
			http.Error(w, "error parsing chunk", http.StatusBadRequest)
			return
		}
		defer file.Close()

		buf := bytes.NewBuffer(nil)
		if _, err := io.Copy(buf, file); err != nil {
			http.Error(w, "error parsing chunk", http.StatusBadRequest)
			return
		}

		_, err = os.Create(filepath.Join(chunkserver.Dir, fileHeader.Filename))
		if err != nil {
			http.Error(w, fmt.Sprintf("error creating file: %v", err), http.StatusInternalServerError)
			return
		}

		chunk := buf.Bytes()
		ioutil.WriteFile(filepath.Join(chunkserver.Dir, fileHeader.Filename), chunk, 0777)

		chunkserver.reportChunkUploadSuccessToMaster(fileHeader.Filename)
	}))
}

func (chunkserver *Chunkserver) registerAtMaster() (*RegisterChunkserverResponse, error) {
	registerReq := RegisterChunkserverRequest{
		Url: fmt.Sprintf("%v:%v/", GetOutboundIP(), chunkserver.Port),
	}

	var registerResponse RegisterChunkserverResponse
	if err := postJson(chunkserver.Master+"chunkserver", &registerReq, &registerResponse); err != nil {
		return nil, err
	}

	return &registerResponse, nil
}

func (chunkserver *Chunkserver) reportChunkUploadSuccessToMaster(chunkIdentifier string) error {
	uploadSuccessfulReq := ChunkUploadSuccessRequest{chunkIdentifier, fmt.Sprintf("%v:%v/", GetOutboundIP(), chunkserver.Port)}
	client := &http.Client{Timeout: 10 * time.Second}
	requestBody, _ := json.Marshal(uploadSuccessfulReq)
	_, err := client.Post(chunkserver.Master+"uploadSuccessful", "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}

	return nil
}

func (chunkserver *Chunkserver) registerGetChunkEndpoint() {
	http.HandleFunc("/get", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		chunkidentifier := r.URL.Query()["id"][0]

		f, err := os.Open(filepath.Join(chunkserver.Dir, chunkidentifier))
		if err != nil {
			http.Error(w, fmt.Sprintf("couldn't open chunk file: %v", err), http.StatusInternalServerError)
			return
		}
		defer f.Close()

		_, err = io.Copy(w, f)
		if err != nil {
			http.Error(w, fmt.Sprintf("error writing chunk data into stream: %v", err), http.StatusInternalServerError)
			return
		}
	}))
}
