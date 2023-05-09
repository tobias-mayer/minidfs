package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

type RegisterChunkserverRequest struct {
	Url string `json:"url"`
}

type RegisterChunkserverResponse struct {
}

type UploadInitRequest struct {
	FileName string `json:"fileName"`
	FileSize uint64 `json:"fileSize"`
}

type UploadInitResponse struct {
	Identifier     string   `json:"identifier"`
	ChunkSize      uint64   `json:"chunkSize"`
	NumberOfChunks uint64   `json:"numberOfChunks"`
	Chunkservers   []string `json:"chunkservers"`
}

type ChunkUploadSuccessRequest struct {
	ChunkIdentifier string `json:"chunkIdentifier"`
	Chunkserver     string `json:"chunkserver"`
}

type GetResponse struct {
	FileName  string
	Locations []string
}

type FileMetadata struct {
	FileName       string
	FileSize       uint64
	NumberOfChunks uint64
	Replicas       [][]string
}

type MasterServer struct {
	Port            string
	ChunkSize       uint64
	ChunkserverUrls map[string]struct{}
	Files           map[string]FileMetadata
}

func NewMasterServer(port string, chunkSize uint64) *MasterServer {
	return &MasterServer{port, chunkSize, make(map[string]struct{}), make(map[string]FileMetadata)}
}

func (master *MasterServer) run() error {
	master.registerUploadEndpoint()
	master.registerGetEndpoint()
	master.registerReportChunkUploadSuccessEndpoint()
	master.registerRegisterChunkserverEndpoint()

	if err := http.ListenAndServe(":"+master.Port, nil); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (master *MasterServer) registerUploadEndpoint() {
	http.HandleFunc("/upload", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var uploadReq UploadInitRequest
		if err := json.NewDecoder(r.Body).Decode(&uploadReq); err != nil {
			http.Error(w, "error parsing request body", 400)
			return
		}

		if len(master.ChunkserverUrls) == 0 {
			http.Error(w, "no chunkserver available", 500)
			return
		}

		fileIdentifier := getIdentifierFromFilename(uploadReq.FileName)
		numberOfChunks := calcNumberOfChunks(uploadReq.FileSize, master.ChunkSize)

		// urls := reflect.ValueOf(master.ChunkserverUrls).MapKeys()
		// randomChunkserver := urls[rand.Intn(len(urls))].String()
		chunkservers := make([]string, 0, len(master.ChunkserverUrls))
		for k := range master.ChunkserverUrls {
			chunkservers = append(chunkservers, k)
		}

		master.Files[fileIdentifier] = FileMetadata{uploadReq.FileName, uploadReq.FileSize, numberOfChunks, make([][]string, numberOfChunks)}

		response := UploadInitResponse{
			Identifier:     fileIdentifier,
			ChunkSize:      master.ChunkSize,
			NumberOfChunks: numberOfChunks,
			Chunkservers:   chunkservers,
		}
		json.NewEncoder(w).Encode(response)
	}))
}

func (master *MasterServer) registerGetEndpoint() {
	http.HandleFunc("/get", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		fileIdentifier := r.URL.Query()["id"][0]
		metadata, ok := master.Files[fileIdentifier]
		if !ok {
			http.Error(w, fmt.Sprintf("file with identifier '%v' not found", fileIdentifier), http.StatusNotFound)
			return
		}

		locations := make([]string, metadata.NumberOfChunks)
		for i, v := range metadata.Replicas {
			if len(v) == 0 {
				http.Error(w, "file exists but not all chunks are currently available", http.StatusNotFound)
				return
			}

			// for now we pick a random chunkserver for each chunk.
			// GFS picks the chunkserver that is closest to the client and does bulk requests
			locations[i] = metadata.Replicas[i][rand.Intn(len(metadata.Replicas[i]))]
		}

		response := GetResponse{
			FileName:  metadata.FileName,
			Locations: locations,
		}
		json.NewEncoder(w).Encode(response)
	}))
}

func (master *MasterServer) registerReportChunkUploadSuccessEndpoint() {
	http.HandleFunc("/uploadSuccessful", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var uploadSuccessReq ChunkUploadSuccessRequest
		if err := json.NewDecoder(r.Body).Decode(&uploadSuccessReq); err != nil {
			http.Error(w, "error parsing request body", 400)
			return
		}

		parts := strings.Split(uploadSuccessReq.ChunkIdentifier, "_")
		fileIdentifier := parts[0]
		chunkIndex, err := strconv.Atoi(parts[1])
		if err != nil {
			http.Error(w, fmt.Sprintf("error parsing chunk index: %v", err), http.StatusBadRequest)
		}

		master.Files[fileIdentifier].Replicas[chunkIndex] = append(master.Files[fileIdentifier].Replicas[chunkIndex], uploadSuccessReq.Chunkserver)
	}))
}

func (master *MasterServer) registerRegisterChunkserverEndpoint() {
	http.HandleFunc("/chunkserver", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		var registerReq RegisterChunkserverRequest
		if err := json.NewDecoder(r.Body).Decode(&registerReq); err != nil {
			http.Error(w, "error parsing request body", 400)
		}

		master.ChunkserverUrls[registerReq.Url] = struct{}{}

		response := RegisterChunkserverResponse{}
		json.NewEncoder(w).Encode(response)
	}))
}
