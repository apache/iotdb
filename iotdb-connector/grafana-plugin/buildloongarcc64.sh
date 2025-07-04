#!/bin/bash

# 清理之前的构建
rm -rf dist/gpx_apache_iotdb_datasource_linux_loong64*

# 手动执行 Go 构建命令（从 Mage 输出中复制并修改）
env GOOS=linux GOARCH=loong64 go build \
  -o "dist/gpx_apache_iotdb_datasource_linux_loong64" \
  -tags "arrow_json_stdlib" \
  -ldflags "-w -s -extldflags \"-static\" -X 'github.com/grafana/grafana-plugin-sdk-go/build.buildInfoJSON={\"time\":1751624588982,\"pluginID\":\"apache-iotdb-datasource\",\"version\":\"1.0.1\"}' -X 'main.pluginID=apache-iotdb-datasource' -X 'main.version=1.0.1'" \
  "./pkg"

echo "LoongArch64 finish: dist/gpx_apache_iotdb_datasource_linux_loong64"
