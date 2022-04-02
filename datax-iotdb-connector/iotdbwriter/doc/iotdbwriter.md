# IOTDBWriter 插件文档

## 1 快速介绍
IOTDBWriter支持将大批量数据写入IOTDB中。

## 2 实现原理
IOTDBWriter 通过IOTDB原生支持Session方式导入数据， IOTDBWriter会将`reader`读取的数据进行缓存在内存中，然后批量导入至IOTDB。

## 3 功能说明

### 3.1 配置样例

这里是一份从Stream读取数据后导入至IOTDB的配置文件。

```
{
  "job": {
    "setting": {
      "speed": {
        "channel": 1
      }
    },
    "content": [
      {
        "reader": {
          "name": "streamreader",
          "parameter": {
            "column": [
              {
                "value": "guangzhou",
                "type": "string"
              },
              {
                "random": "2014-07-07 00:00:00, 2016-07-07 00:00:00",
                "type": "date"
              },
              {
                "random": "0, 19890604",
                "type": "long"
              },
              {
                "random": "2, 10",
                "type": "bool"
              },
              {
                "random": "10, 23410",
                "type": "DOUBLE"
              }
            ],
            "sliceRecordCount": 100000
          }
        },
        "writer": {
          "name": "iotdbwriter",
          "parameter": {
            "column": [
              {
                "name": "loc",
                "type": "text"
              },
              {
                "name": "timeseries",
                "type": "date"
              },
              {
                "name": "s2",
                "type": "int64"
              },
              {
                "name": "s3",
                "type": "boolean"
              },
              {
                "name": "s4",
                "type": "double"
              }
            ],
            "username": "root",
            "password": "root",
            "host": "localhost",
            "port": 6667,
            "deviceId": "root.sg4.d1",
            "batchSize": 1000
          }
        }
      }
    ]
  }
}

```

### 3.2 参数说明

