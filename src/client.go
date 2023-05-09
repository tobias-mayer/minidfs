package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"time"
)

type Client struct {
	Master         string
	Action         string
	Filename       string
	OutputFilename string
}

func NewClient(master string, action string, filename string, outputFilename string) *Client {
	return &Client{master, action, filename, outputFilename}
}

func (client *Client) run() error {
	if client.Action == "write" {
		file, err := os.Open(client.Filename)
		if err != nil {
			log.Fatal(err)
		}

		defer file.Close()

		fileInfo, _ := file.Stat()
		fileSize := fileInfo.Size()

		uploadInitResponse, err := client.initUpload(client.Master, client.Filename, uint64(fileSize))
		if err != nil {
			log.Fatalf("initializing the upload failed: %v", err)
		}

		client.uploadToChunkservers(uploadInitResponse, file, fileSize, uploadInitResponse.Identifier)
	} else {
		getResponse, err := client.initGet()
		if err != nil {
			log.Fatalf(fmt.Sprintf("error getting metadata from master %v", err))
		}

		log.Println(getResponse)

		client.getChunks(getResponse.Locations)
	}

	return nil
}

func (client *Client) initUpload(masterUrl string, filename string, filesize uint64) (*UploadInitResponse, error) {
	uploadRequest := UploadInitRequest{
		FileName: filename,
		FileSize: filesize,
	}
	var uploadResponse UploadInitResponse
	err := postJson(masterUrl+"upload", &uploadRequest, &uploadResponse)
	if err != nil {
		return nil, err
	}

	log.Println(uploadResponse)

	return &uploadResponse, nil
}

func (client *Client) uploadToChunkservers(uploadInitResponse *UploadInitResponse, file *os.File, fileSize int64, identifier string) {
	log.Printf("splitting input file into %d chunks of size %d bytes", uploadInitResponse.NumberOfChunks, uploadInitResponse.ChunkSize)

	for i := uint64(0); i < uploadInitResponse.NumberOfChunks; i++ {
		partSize := int(math.Min(float64(uploadInitResponse.ChunkSize), float64(fileSize-int64(i*uploadInitResponse.ChunkSize))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		randomChunkserver := uploadInitResponse.Chunkservers[rand.Intn(len(uploadInitResponse.Chunkservers))]
		if err := client.uploadChunk(randomChunkserver, &partBuffer, fmt.Sprintf("%v_%v", identifier, i)); err != nil {
			log.Fatalf("error while uploading chunk: %v", err)
		}

		log.Println(partBuffer[:10])
	}
}

func (client *Client) uploadChunk(chunkserverUrl string, chunk *[]byte, chunkIdentifier string) error {
	c := &http.Client{Timeout: 10 * time.Second}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("chunk", chunkIdentifier)
	io.Copy(part, bytes.NewReader(*chunk))
	writer.Close()

	req, err := http.NewRequest("POST", "http://"+chunkserverUrl+"uploadChunk", body)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", writer.FormDataContentType())

	response, _ := c.Do(req)
	if response.StatusCode != http.StatusOK {
		log.Printf("Request failed with response code: %d", response.StatusCode)
	}

	return nil
}

func (client *Client) initGet() (*GetResponse, error) {
	req, err := http.NewRequest("GET", client.Master+"get", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("id", getIdentifierFromFilename(client.Filename))
	req.URL.RawQuery = q.Encode()

	c := &http.Client{Timeout: 10 * time.Second}
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var getResponse GetResponse
	if err := json.NewDecoder(res.Body).Decode(&getResponse); err != nil {
		return nil, err
	}

	return &getResponse, nil
}

func (client *Client) getChunks(locations []string) error {
	fileIdentifier := getIdentifierFromFilename(client.Filename)

	file, err := os.Create(client.OutputFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	for i, chunkserver := range locations {
		chunkIdentifier := fmt.Sprintf("%v_%v", fileIdentifier, i)

		chunk, err := client.getChunk(chunkIdentifier, chunkserver)
		if err != nil {
			log.Fatal(fmt.Sprintf("couln't download chunk: %v", err))
		}

		file.Write(chunk)
	}

	return nil
}

func (client *Client) getChunk(chunkIdentifier string, chunkserver string) ([]byte, error) {
	req, err := http.NewRequest("GET", "http://"+chunkserver+"get", nil)
	if err != nil {
		return nil, err
	}

	q := req.URL.Query()
	q.Add("id", chunkIdentifier)
	req.URL.RawQuery = q.Encode()

	c := &http.Client{Timeout: 10 * time.Second}
	res, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	chunk, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	return chunk, nil
}
