/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package log

import (
	"fmt"
	"log"
	"os"
	"strings"
)

var level = 5

func getTag() string {
	s := strings.Split(os.Args[0], "/") //[2:3]
	l := len(s)
	v := s[l-1 : l][0]
	vv := v + "                    "
	return fmt.Sprintf("[ %.15s.. ] ", vv)
}

var tag = getTag()

func SetLevel(l int) {
	level = l
}

func Debuga(format string) {
	log.Printf("%8.2s : %s", "debug", format)
}

func Debug(s string, v ...interface{}) {
	if level > 1 {
		return
	}
	fmt.Print("\033[32m")
	fmt.Print(tag)
	log.Print(" DEBUG:", fmt.Sprintf(s, v...))
	fmt.Print("\033[0m")
}
func Info(s string, v ...interface{}) {
	if level > 3 {
		return
	}
	fmt.Print("\033[34m")
	//fmt.Print(tag)
	//log.SetFlags(log.Lshortfile)
	log.Print("  INFO: ", fmt.Sprintf(s, v...))
	fmt.Print("\033[0m")
}

func Error(s string, v ...interface{}) {
	if level > 5 {
		return
	}
	fmt.Print("\033[31m")
	fmt.Print(tag)
	log.Print(" ERROR:", fmt.Sprintf(s, v...))
	fmt.Print("\033[0m")
}
