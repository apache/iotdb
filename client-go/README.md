# client-go-iotdb

## Generate Thrift Code
1. pull docker image `thrift`
2. copy latest `*.thritf` files into directory 'thrift'
3. Run `sh gen_thrift.sh` under directory 'thrift'
4. Move the `thrift/gen-go` to root directory

## Run Demo
1. edit main.go to set host and port ... of your iotdb server
2. run command:
``` bash
go run main.go
```
