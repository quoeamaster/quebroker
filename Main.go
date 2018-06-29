package main

import (
    "gopkg.in/urfave/cli.v1"
    "os"
    "log"
    "fmt"
)

func main() {
    queBroker := cli.NewApp()
    queBroker.Name = "Que Broker"
    queBroker.Usage = "entry point of the broker component"
    queBroker.Description = "the broker for Que Message system"
    queBroker.Version = "0.9.0-alpha"

    queBroker.Action = func(ctx *cli.Context) error {
        fmt.Println("** inside action")
        return nil
    }


    err := queBroker.Run(os.Args)
    if err != nil {
        log.Fatal(err)
    }
}
