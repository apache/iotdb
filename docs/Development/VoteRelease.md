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

# How to vote for a release

For non-Chinese users, please read

https://cwiki.apache.org/confluence/display/IOTDB/Validating+a+staged+Release

## Download everything under voting version / rc

https://dist.apache.org/repos/dist/dev/iotdb/

## Import the public key of the release manager

https://dist.apache.org/repos/dist/dev/iotdb/KEYS

At the bottom is the public key of the Release Manager (RM)

Install gpg2

### the first method

```
The beginning of the public key is this
pub   rsa4096 2019-10-15 [SC]
      10F3B3F8A1201B79AA43F2E00FC7F131CAA00430
      
Or this

pub   rsa4096/28662AC6 2019-12-23 [SC]
```

Download the public key

```
gpg2 --receive-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 (or 28662AC6)

or (Designation keyserver) 
gpg2 --keyserver p80.pool.sks-keyservers.net --recv-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 (或 28662AC6)
```

### The second method

Copy the following paragraph into a text file and name it `key.asc`

```
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: GnuPG v2
...
-----END PGP PUBLIC KEY BLOCK-----
```

Import RM's public key to your computer

```
gpg2 --import key.asc
```

## Verify the source distribution

* Verify that there are  NOTICE, LICENSE, and the content is correct.

* Verify README, RELEASE_NOTES

* Validation header

```
mvn -B apache-rat:check
```

* Verify signatures and hashes

```
gpg2 --verify apache-iotdb-0.12.0-source-release.zip.asc apache-iotdb-0.12.0-source-release.zip

appear Good Singnature 

shasum -a512 apache-iotdb-0.12.0-source-release.zip

Compared with the corresponding .sha512, the same is fine.
```

* Verify compilation

```
mvnw install

Should end up all SUCCESS
```

## Verifying the binary release

* Verify that there are NOTICE, LICENSE, and the content is correct.

* Verify README, RELEASE_NOTES

* Verify signatures and hashes

```
gpg2 --verify apache-iotdb-0.12.0-bin.zip.asc apache-iotdb-0.12.0-bin.zip

appear Good Singnature 

shasum -a512 apache-iotdb-0.12.0-bin.zip

Compared with the corresponding .sha512, the same is fine.
```

* Verify that it starts and the sample statements execute correctly

```
nohup ./sbin/start-server.sh >/dev/null 2>&1 &

./sbin/start-cli.sh

CREATE DATABASE root.turbine;
CREATE TIMESERIES root.turbine.d1.s0 WITH DATATYPE=DOUBLE, ENCODING=GORILLA;
insert into root.turbine.d1(timestamp,s0) values(1,1);
insert into root.turbine.d1(timestamp,s0) values(2,2);
insert into root.turbine.d1(timestamp,s0) values(3,3);
select * from root;

Prints the following:
+-----------------------------------+------------------+
|                               Time|root.turbine.d1.s0|
+-----------------------------------+------------------+
|      1970-01-01T08:00:00.001+08:00|               1.0|
|      1970-01-01T08:00:00.002+08:00|               2.0|
|      1970-01-01T08:00:00.003+08:00|               3.0|
+-----------------------------------+------------------+

```

## Sample mail

Email can be sent after verification

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
select * from root;

Thanks,
xxx
```


## small tools

* Print out lines containing certain characters (just look at the top output, you don't need to look at the bottom file)

```
find . -type f -exec grep -i "copyright" {} \; -print | sort -u
find **/src -type f -exec grep -i "copyright" {} \; -print | sort -u
```
