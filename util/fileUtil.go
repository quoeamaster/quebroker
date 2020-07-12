// Licensed to quoeamaster@gmail.com under one or more contributor
// license agreements. See the LICENSE file distributed with
// this work for additional information regarding copyright
// ownership. quoeamaster@gmail.com licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package util

import (
	"fmt"
	"os"
	"strings"
)

// IsFileExists - checks whether a file exists based on the given path (mandatory) and optional subpaths
func IsFileExists(path string, paths ...string) (exists bool, configPath string) {
	_path := path
	if paths != nil {
		if strings.Compare(_path, "") > 0 {
			_path = fmt.Sprintf("%v%v%v", _path, string(os.PathSeparator), strings.Join(paths, ""))
		} else {
			_path = strings.Join(paths, "")
		}
	}
	//fmt.Println("toml config path", _path, "*")
	_, err := os.Stat(_path)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	// all good
	exists = true
	configPath = _path
	return
}

// LoadFileContents - load the contents of a given file
