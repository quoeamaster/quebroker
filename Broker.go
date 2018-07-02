/*
 *  Copyright Project - queBroker, Author - quoeamaster, (C) 2018
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package main

import (
    "queutil"
    "github.com/emicklei/go-restful"
    "os"
    "os/signal"
    "syscall"
    "fmt"
    "sync"
    "net/http"
    "strings"
    "runtime"
)

// when the broker is not yet started up; this file indicated the broker's UUID
const brokerIdFile = "broker.id"

// after the broker started, the original broker.id file is renamed as
// broker.id.lock and "locked" up for updating
const brokerIdLockFile = "broker.id.lock"

// lock for the Broker singleton creation
var syncLock sync.Once
var brokerInstance *Broker


type Broker struct {
    // UUID for the broker (must be unique within a cluster)
    UUID string

    // broker config wrapper
    config *BrokerConfig

    // the locked broker.id.lock file
    //lockedBrokerIdFile lockfile.Lockfile

    // container for the RESTful-abled server
    webserver *restful.Container
}

func GetBroker(configPath string) *Broker {
    syncLock.Do(func() {
        var err error
        brokerInstance, err = newBroker(configPath)
        if err != nil {
            // the only way to let the outside world know is "panic"
            panic(err)
        }
    })
    return brokerInstance
}

func newBroker(configPath string) (*Broker, error) {
    m := new(Broker)

    // config loading
    configInstancePtr, err := NewBrokerConfig(configPath)
    if err != nil {
        return nil, err
    }
    m.config = configInstancePtr

    // uuid
    _, uuid, err := m.getBrokerUUID()
    if err != nil {
        return nil, err
    }
    m.UUID = uuid

    // restful.Container, add modules to implement REST api(s) later on
    m.webserver = restful.NewContainer()

    return m, nil
}

// return the broker's UUID. 2 situations arise:
//
//  a) 1st time running the broker; create the UUID and store it inside "broker.id" file
//  b) "broker.id" file already existed, read back the UUID from the file instead
//
// return 3 information:
//  1) bool => is the broker started for the 1st time (true)
//  2) uuid => broker's uuid (could be generated or read from broker.id)
//  3) error => any error occurred
func (b *Broker) getBrokerUUID() (bool, string, error) {
    // check if broker.id.lock or broker.id file is there or not
    if queutil.IsFileExists(brokerIdFile) == true {
        uuidByteArr, err := queutil.ReadFileContent(brokerIdFile)
        if err != nil {
            return false, "", err
        }
        return false, string(uuidByteArr), nil

    } else {
        // assume no broker UUID (first time starting up the broker)
        uuid := queutil.GenerateUUID()
        err := queutil.WriteStringToFile(brokerIdFile, uuid)
        if err != nil {
            return false, "", err
        }
        return true, uuid, nil
    }
}

func (b *Broker) StartBroker() error {
    // bootstrap logs

    // a) start the routine to listen for exit signals
    go b.listenToExitSequences()

    // b) add api modules
    b.webserver.Add(NewStatsApiModule())

    // c) rename broker.id => broker.id.lock and LOCK the file as well
    err := queutil.RenameFile(brokerIdFile, brokerIdLockFile, 0444)
    if err != nil {
        return err
    }
    lockFilePath, err := b.appendFileToWD(brokerIdLockFile)
    if err != nil {
        return err
    }
    err = queutil.LockFile(lockFilePath)
    if err != nil {
        fmt.Printf("trying to lock [%v]\n", lockFilePath)
        return err
    }


    // d) start the http server
    return http.ListenAndServe(b.config.BrokerCommunicationAddress, b.webserver)
}

func (b *Broker) appendFileToWD(file string) (string, error) {
    slash := "/"
    if strings.Compare(runtime.GOOS, "windows") == 0 {
        slash = "\\"
    }
    path, err := os.Getwd()
    if err != nil {
        return "", err
    }
    return fmt.Sprintf("%v%v%v", path, slash, file), nil
}

func (b *Broker) Release(optionalParams map[string]interface{}) error {
    // resource release here

    err := queutil.RenameFile(brokerIdLockFile, brokerIdFile, 0444)
    if err != nil {
        return err
    }
    lockFilePath, err := b.appendFileToWD(brokerIdFile)
    if err != nil {
        return err
    }
    err = queutil.UnlockFile(lockFilePath)
    if err != nil {
        fmt.Printf("trying to unlock [%v] but got exception => [%v]\n", lockFilePath, err)
        return err
    }


    return nil
}

// method to listen to lifecycle hooks based on signal-term or signal-interrupt
func (b *Broker) listenToExitSequences() {
    signalChannel := make(chan os.Signal, 1)
    signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)

    sig := <- signalChannel
    fmt.Println("sig received =>", sig)
    // call cleanup method(s)
    err := b.Release(nil)
    if err != nil {
        fmt.Println(err)
    }
    os.Exit(1)
}

func (b *Broker) AddModules(module *restful.WebService) error {
    if module == nil {
        return fmt.Errorf("invalid module supplied~ [%v]", module)
    }
    b.webserver.Add(module)

    return nil
}
