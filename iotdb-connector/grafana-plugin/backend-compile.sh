#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Check if go is installed
exists_env=$(go version | grep -c "go version")
if [ "$exists_env" -eq 0 ]; then
    echo "Need to install go environment"
    exit 1
fi

work_path=$(pwd | sed 's/\"//g')
echo work_path="$work_path" || exit
# List the current go environment configuration, take the line that starts with
# "GOPATH=" and remove all single or double-quotes from that.
check_results=$(go env | grep GOPATH= | sed "s/['\"]//g")
go_path=${check_results/GOPATH=/}
echo GOPATH="$go_path"

# Get the grafana-plugin-sdk (This also fetches mage 1.15.0)
go get -u github.com/grafana/grafana-plugin-sdk-go@v0.193.0
# As we might have updated the Grafana plugin dependency, update the go.sum file
go mod tidy

# Bootstrap Mage (which should compile and install it)
cd "$go_path"/pkg/mod/github.com/magefile/mage@v1.15.0 || exit
chmod 755 "$go_path"/pkg/mod/github.com/magefile/*
go run "$go_path"/pkg/mod/github.com/magefile/mage@v1.15.0/bootstrap.go
cd "$work_path" || exit

# Now build the plugin using Mage
$go_path/bin/mage -v
