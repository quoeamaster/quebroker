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
    // setup flags (e.g. ENV_VAR)
    queBroker.Flags = []cli.Flag {
        cli.StringFlag{
          Name: "config, C",
          Value: localBrokerConfigPath,
          Usage: "path of the broker config (toml format)",
          EnvVar: envVarBrokerConfigPath,
        },
    }

    queBroker.Action = func(ctx *cli.Context) error {
        brokerInstance, err := NewBroker(ctx.String("C"))
        if err != nil {
            panic(err)
        }
        fmt.Println(brokerInstance.config.GetPath())
        fmt.Println(brokerInstance.config.BrokerName)

        return nil
    }


    err := queBroker.Run(os.Args)
    if err != nil {
        log.Fatal(err)
    }
}
