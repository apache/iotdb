<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Go 原生接口

### 依赖

 * golang >= 1.13
 * make   >= 3.0
 * curl   >= 7.1.1
 * thrift 0.13.x
 * Linux、Macos或其他类unix系统
 * Windows+bash(WSL、cygwin、Git Bash)




### 安装方法

 * 通过go mod

```sh
# 切换到GOPATH的HOME路径，启用Go Modules功能
export GO111MODULE=on

# 配置GOPROXY环境变量
export GOPROXY=https://goproxy.io

# 创建命名的文件夹或目录，并切换当前目录
mkdir session_example && cd session_example

# 保存文件，自动跳转到新的地址
curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go

# 初始化go module环境
go mod init session_example

# 下载依赖包
go mod tidy

# 编译并运行程序
go run session_example.go
```

* 通过GOPATH

```sh
# get thrift 0.13.0
go get github.com/apache/thrift@0.13.0

# 递归创建目录
mkdir -p $GOPATH/src/iotdb-client-go-example/session_example

# 切换到当前目录
cd $GOPATH/src/iotdb-client-go-example/session_example

# 保存文件，自动跳转到新的地址
curl -o session_example.go -L https://github.com/apache/iotdb-client-go/raw/main/example/session_example.go

# 初始化go module 环境
go mod init

# 下载依赖包
go mod tidy

# 编译并运行程序
go run session_example.go
```

