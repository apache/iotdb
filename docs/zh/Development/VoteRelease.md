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

# 给发布版本投票

For non-Chinese users, please read https://cwiki.apache.org/confluence/display/IOTDB/Validating+a+staged+Release

## 下载投票的 版本/rc 下的所有内容

https://dist.apache.org/repos/dist/dev/iotdb/

## 导入发布经理的公钥

https://dist.apache.org/repos/dist/dev/iotdb/KEYS

最下边有 Release Manager (RM) 的公钥

安装 gpg2

### 第一种方法

```
公钥的开头是这种
pub   rsa4096 2019-10-15 [SC]
      10F3B3F8A1201B79AA43F2E00FC7F131CAA00430
      
或这种

pub   rsa4096/28662AC6 2019-12-23 [SC]
```

下载公钥

```
gpg2 --receive-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 （或 28662AC6)

或 （指定 keyserver) 
gpg2 --keyserver p80.pool.sks-keyservers.net --recv-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 （或 28662AC6)
```

### 第二种方法

把下边这段复制到一个文本文件中，起个名叫 ```key.asc```

```
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2
...
-----END PGP PUBLIC KEY BLOCK-----
```

导入 RM 的公钥到自己电脑

```
gpg2 --import key.asc
```

## 验证源码发布版

* 验证是否有 NOTICE、LICENSE，以及内容是否正确。

* 验证 README、RELEASE_NOTES

* 验证 header

```
mvn -B apache-rat:check
```

* 验证签名和哈希值

```
gpg2 --verify apache-iotdb-0.12.0-source-release.zip.asc apache-iotdb-0.12.0-source-release.zip

出现 Good Singnature 

shasum -a512 apache-iotdb-0.12.0-source-release.zip

和对应的 .sha512 对比，一样就可以。
```

* 验证编译

```
mvnw install

应该最后全 SUCCESS
```

## 验证二进制发布版

* 验证是否有 NOTICE、LICENSE，以及内容是否正确。

* 验证 README、RELEASE_NOTES

* 验证签名和哈希值

```
gpg2 --verify apache-iotdb-0.12.0-bin.zip.asc apache-iotdb-0.12.0-bin.zip

出现 Good Singnature 

shasum -a512 apache-iotdb-0.12.0-bin.zip

和对应的 .sha512 对比，一样就可以。
```

* 验证是否能启动以及示例语句是否正确执行

```
nohup ./sbin/start-server.sh >/dev/null 2>&1 &

./sbin/start-cli.sh

CREATE DATABASE root.turbine;
CREATE TIMESERIES root.turbine.d1.s0 WITH DATATYPE=DOUBLE, ENCODING=GORILLA;
insert into root.turbine.d1(timestamp,s0) values(1,1);
insert into root.turbine.d1(timestamp,s0) values(2,2);
insert into root.turbine.d1(timestamp,s0) values(3,3);
select * from root.**;

打印如下内容：
+-----------------------------------+------------------+
|                               Time|root.turbine.d1.s0|
+-----------------------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|               1.0|
|      1970-01-01T08:00:00.002+08:00|               2.0|
|      1970-01-01T08:00:00.003+08:00|               3.0|
+-----------------------------------+------------------+

```

## 示例邮件

验证通过之后可以发邮件了

```
Hi,

+1 (PMC could binding)

The source release:
LICENSE and NOTICE [ok]
signatures and hashes [ok]
All files have ASF header [ok]
could compile from source: ./mvnw clean install [ok]

The binary distribution:
LICENSE and NOTICE [ok]
signatures and hashes [ok]
Could run with the following statements [ok]

CREATE DATABASE root.turbine;
CREATE TIMESERIES root.turbine.d1.s0 WITH DATATYPE=DOUBLE, ENCODING=GORILLA;
insert into root.turbine.d1(timestamp,s0) values(1,1);
insert into root.turbine.d1(timestamp,s0) values(2,2);
insert into root.turbine.d1(timestamp,s0) values(3,3);
select * from root.**;

Thanks,
xxx
```

## 小工具

* 打印出包含某些字符的行（只看最上边的输出就可以，下边的文件不需要看）

```
find . -type f -exec grep -i "copyright" {} \; -print | sort -u
find **/src -type f -exec grep -i "copyright" {} \; -print | sort -u
```