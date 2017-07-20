TsFile  Load脚本使用说明
# 使用方法

##创建MetaData(自定义创建，样例为测试数据metadata)
CREATE TIMESERIES root.fit.d1.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d1.s2 WITH DATATYPE=BYTE_ARRAY,ENCODING=PLAIN;
CREATE TIMESERIES root.fit.d2.s1 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.d2.s3 WITH DATATYPE=INT32,ENCODING=RLE;
CREATE TIMESERIES root.fit.p.s1 WITH DATATYPE=INT32,ENCODING=RLE;
SET STORAGE GROUP TO root.fit.d1;

## 启动load脚本

> ./bin/start-csvToTsfile.sh.sh -h 127.0.0.1 -p 6667 -u root -pw root -f default-long.csv -t timestamps
usage: CSV_To_TsFile [-f <file>] [-h <host>] [-help] [-p <port>] [-pw <password>] [-t
       <timeformat>] [-u <username>]
 -f <file>         csv file path (required)
 -h <host>         Host Name (required)
 -help             Display help information
 -p <port>         Port (required)
 -pw <password>    Password (required)
 -t <timeformat>   timeFormat  (not required) format: timestamps, ISO8601, "yyyy-MM-dd HH:mm:ss"(自定义格式)
 -u <username>     User name (required)
