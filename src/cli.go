package main

import (
	"log"
	"os"
	"sort"
	"strings"

	"github.com/urfave/cli"
)

func main() {
	app := &cli.App{
		Name:  "minidfs",
		Usage: "Example: cli [COMMAND]",
		Commands: []cli.Command{
			{
				Name:  "master",
				Usage: "start the master server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "port",
						Usage: "Port to run the server on",
					},
					&cli.IntFlag{
						Name:  "chunkSize",
						Usage: "Chunk size",
					},
				},
				Action: func(c *cli.Context) error {
					port := c.String("port")
					if port == "" {
						log.Fatal("please specify a port using '--port 8000'")
					}

					chunkSize := c.Uint64("chunkSize")
					if chunkSize == 0 {
						log.Fatal("please specify a chunk size using '--chunkSize 1024'")
					}

					master := NewMasterServer(port, chunkSize)
					return master.run()
				},
			},
			{
				Name:  "chunkserver",
				Usage: "start a instance of the chunkserver",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "port",
						Usage: "Port to run the server on",
					},
					&cli.StringFlag{
						Name:  "master",
						Usage: "Master server url",
					},
					&cli.StringFlag{
						Name:  "dir",
						Usage: "Directory to store the files in",
					},
				},
				Action: func(c *cli.Context) error {
					port := c.String("port")
					if port == "" {
						log.Fatal("please specify a port using '--port 8001'")
					}

					master := c.String("master")
					if master == "" {
						log.Fatal("please specify the master url using '--master http://localhost:8000'")
					}

					dir := c.String("dir")
					if dir == "" {
						log.Fatal("please specify a valid directory using '--dir ./data'")
					}

					chunkserver := NewChunkserver(master, port, dir)
					return chunkserver.run()
				},
			},
			{
				Name:  "client",
				Usage: "interact with the dfs",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:  "master",
						Usage: "Master server url",
					},
					&cli.StringFlag{
						Name:  "action",
						Usage: "action to perform (read/write)",
					},
					&cli.StringFlag{
						Name:  "filename",
						Usage: "input file name",
					},
					&cli.StringFlag{
						Name:  "output-filename",
						Usage: "output file name",
					},
				},
				Action: func(c *cli.Context) error {
					master := c.String("master")
					if master == "" {
						log.Fatal("please specify the master url using '--master http://localhost:8000'")
					}

					action := strings.ToLower(c.String("action"))
					if action != "read" && action != "write" {
						log.Fatal("action needs to specified as read or write using '--action read'")
					}

					filename := c.String("filename")
					if filename == "" {
						log.Fatal("please specify a filename using '--filename input.txt'")
					}

					outputFilename := c.String("output-filename")
					if action == "read" && outputFilename == "" {
						log.Fatal("please specify a output filename using '--output-filename output.txt' for the read action")
					}

					client := NewClient(master, action, filename, outputFilename)
					return client.run()
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
