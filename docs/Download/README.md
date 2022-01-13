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

# Release version

<table>
	<tr>
      <th>Version</th>
	    <th colspan="3">IoTDB Binaries</th>
	    <th colspan="3">IoTDB Sources</th>
	    <th>release notes</th>  
	</tr>
	<tr>
            <td rowspan="4">0.12.4</td>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip">All-in-one</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-all-bin.zip.asc">ASC</a></td>
            <td rowspan="4"><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip">Sources</a></td>
            <td rowspan="4"><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip.sha512">SHA512</a></td>
            <td rowspan="4"><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-source-release.zip.asc">ASC</a></td>
            <td rowspan="4"><a href="https://raw.githubusercontent.com/apache/iotdb/v0.12.4/RELEASE_NOTES.md">release notes</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip">Server</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-server-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip">Cluster</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-cluster-bin.zip.asc">ASC</a></td>
      </tr>
      <tr>
            <td><a href="https://www.apache.org/dyn/closer.cgi/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip">Grafana-connector</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip.sha512">SHA512</a></td>
            <td><a href="https://downloads.apache.org/iotdb/0.12.4/apache-iotdb-0.12.4-grafana-bin.zip.asc">ASC</a></td>
      </tr>
</table>

Legacy version are available here: [https://archive.apache.org/dist/iotdb/](https://archive.apache.org/dist/iotdb/)


**<font color=red>Attention</font>**:

- Recommended OS parameters
  * Set the somaxconn as 65535 to avoid "connection reset" error when the system is under high load.
    ```
    # Linux
    > sudo sysctl -w net.core.somaxconn=65535
   
    # FreeBSD or Darwin
    > sudo sysctl -w kern.ipc.somaxconn=65535
    ```

- How to upgrade a minor version (e.g., from v0.11.0 to v0.11.3)?
  * versions which have the same major version are compatible.
  * Just download and unzip the new version. Then modify the configuration files to keep consistent 
  with what you set in the old version.
  * stop the old vesion instance, and start the new one.

- How to upgrade from v.11.x or v0.10.x to v0.12.x?
  * Upgrading from v0.11 or v0.10 to v0.12 is similar as v0.9 to v0.10. The upgrade tool will rewrite the data files automatically.
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using sbin/start-cli.sh in original version to close all TsFiles.
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.12.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
  directories point to the folders set in previous version (or the backup folder). 
  You can also modify other settings if you want. Any other config changes in v0.11 should be moved to v0.12. 
  * Stop IoTDB v0.11 or v0.10 instance, and start v0.12.x, then the IoTDB will upgrade data file format automatically. It is ok to read and write data when the upgrading process works.
  * After a log `All files upgraded successfully!` printed, the upgrading completes.
  * __NOTICE 1: V0.12 changes many settings in conf/iotdb-engine.properties, so do not use previous 
    configuration file directly.__
  * __NOTICE 2: V0.12 doesn't support upgrade from v0.9 or lower version, please upgrade to v0.10 first if needed.__
  * __NOTICE 3: We don't recommend to delete data before the upgrading finished. The deletion will fail if you try to delete data in the storage group with upgrading files.__

- How to upgrade from v.10.x to v0.11.x?
  * The data format (i.e., TsFile data) of v0.10.x and v0.11 are compatible, but the WAL file is 
  incompatible. So, you can follow the steps:
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using `sbin/start-cli.sh` in v0.10.x to close all TsFiles.
  * We recommend to backup the the wal files and mlog.txt before upgrading for rolling back.
  * Just download, unzip v0.11.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
    directories point to the data folder set in v0.10.x (or the backup folder). You can also modify 
    other settings if you want.
  * Stop IoTDB v0.10.x instance, and start v0.11.x, then the IoTDB will upgrade data file format 
    automatically.
  * __NOTICE: V0.11 changes many settings in conf/iotdb-engine.properties, so do not use v0.10's 
    configuration file directly.__

- How to upgrade from v0.9.x to v0.10.x?
  * Upgrading from v0.9 to v0.10 is more complex than v0.8 to v0.9.
  * **<font color=red>Stop writing new data.</font>**
  * Call `flush` command using sbin/start-client.sh in v0.9 to close all TsFiles.
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.10.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
  directories point to the folders set in v0.9.x  (or the backup folder). 
  You can also modify other settings if you want. 
  * Stop IoTDB v0.9 instance, and start v0.10.x, then the IoTDB will upgrade data file format automatically.

- How to upgrade from 0.8.x to v0.9.x?
  * We recommend to backup the data file (also the wal files and mlog.txt) before upgrading for rolling back.
  * Just download, unzip v0.9.x.zip, and modify conf/iotdb-engine.proeprties to let all the 
  directories point to the folders set in v0.8.x (or the backup folder). 
  You can also modify other settings if you want. 
  * Stop IoTDB v0.8 instance, and start v0.9.x, then the IoTDB will upgrade data file format automatically.
  


       

# All releases

Find all releases in the [Archive repository](https://archive.apache.org/dist/iotdb/).



# Verifying Hashes and Signatures

Along with our releases, we also provide sha512 hashes in *.sha512 files and cryptographic signatures in *.asc files. The Apache Software Foundation has an extensive tutorial to [verify hashes and signatures ](http://www.apache.org/info/verification.html)which you can follow by using any of these release-signing [KEYS ](https://downloads.apache.org/iotdb/KEYS).
