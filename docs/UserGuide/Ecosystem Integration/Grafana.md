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
# Ecosystem Integration

## Grafana-IoTDB

Grafana is an open source volume metrics monitoring and visualization tool, which can be used to display time series data and application runtime analysis. Grafana supports Graphite, InfluxDB and other major time series databases as data sources. In the IoTDB project, we developed the Grafana connector IoTDB-Grafana, which displays the time series data in the IoTDB, and provides you with a visualization method for using Grafana to display the time series data in the IoTDB database .

### Grafana installation and deployment

#### Install Grafana

* Download url: https://grafana.com/grafana/download
* version >= 4.4.1

#### (simple-json-datasource) Install data source plugin

* plugin name: simple-json-datasource
* Download url: https://github.com/grafana/simple-json-datasource

The specific download method is: Go to the plugin directory of Grafana: `{Grafana file directory}\data\plugins\` (Windows system) or `/var/lib/grafana/plugins` (Linux system) or `/usr/local/var/lib/grafana/plugins` (MacOS system)

For Windows, the data/plugins directory will be automatically created when users start Grafana.

For Linux, the plugins directory needs to be created manually.

For MacOS, see the location prompt on the command line after installing Grafana by using brew install.

Execute the following command:
```
Shell > git clone https://github.com/grafana/simple-json-datasource.git
```

After downloading this plugin, use the grafana-cli tool to install SimpleJson from the commandline:

```
grafana-cli plugins install grafana-simple-json-datasource
```

Alternatively, manually download the .zip file and unpack it into grafana plugins directory.

* `{grafana-install-directory}\data\plugins\` (Windows)
* `/var/lib/grafana/plugins` (Linux)
* `/usr/local/var/lib/grafana/plugins`(Mac)

##### Linux system

```
sudo grafana-cli plugins install grafana-simple-json-datasource
sudo service grafana-server restart
```

##### Continue operation

Then you need to restart grafana server, then you can use browser to visit grafana.

If you see "SimpleJson" in "Type" of "Add data source" pages, then it is install successfully.

Or, if you meet following errors:

```
Unsigned plugins were found during plugin initialization. Grafana Labs cannot guarantee the integrity of these plugins. We recommend only using signed plugins.
The following plugins are disabled and not shown in the list below:
```

Please try to find config file of grafana(eg. customer.ini in windows, and /etc/grafana/grafana.ini in linux), then add following configuration:

```
allow_loading_unsigned_plugins = "grafana-simple-json-datasource"
```

#### Start Grafana

Go to the installation directory of Grafana and start Grafana with the following commands:

* Windows：

```
Shell > bin\grafana-server.exe
```

* Linux：

```
Shell > sudo service grafana-server start
```

* MacOS：

```
Shell > grafana-server --config=/usr/local/etc/grafana/grafana.ini --homepath /usr/local/share/grafana cfg:default.paths.logs=/usr/local/var/log/grafana cfg:default.paths.data=/usr/local/var/lib/grafana cfg:default.paths.plugins=/usr/local/var/lib/grafana/plugins
```

See more information [here](http://docs.grafana.org/installation/windows/).

### IoTDB installation

See [https://github.com/apache/iotdb](https://github.com/apache/iotdb)

### IoTDB-Grafana installation

```shell
git clone https://github.com/apache/iotdb.git
```

### Start IoTDB-Grafana

* Option one(suitable for developers)

Import the entire project, after the maven dependency is installed, directly run`iotdb/grafana/rc/main/java/org/apache/iotdb/web/grafana`directory` TsfileWebDemoApplication.java`, this grafana connector is developed by springboot.

* Option two(suitable for users)

In `/grafana/target/`directory 

```shell
cd iotdb
mvn clean package -pl grafana -am -Dmaven.test.skip=true
cd grafana/target
java -jar iotdb-grafana-{version}.war
```

If following output is displayed, then iotdb-grafana connector is successfully activated.

```shell
$ java -jar iotdb-grafana-{version}.war

  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::        (v1.5.4.RELEASE)
...
```

To configure properties, move the `grafana/src/main/resources/application.properties` to the same directory as the war package (`grafana/target`)

### Explore in Grafana

Grafana displays data for you in the form of a web dashboard. When using it, please open your browser and visit http://<ip>:<port>.
    
The default port of Grafana is 3000, see http://localhost:3000/

Note: IP is the IP of the server where your Grafana is located, and Port is the running port of Grafana (3000 by default). The default login user name and password are both "admin".

#### Add IoTDB data source

Click on the "Grafana" icon in the top left corner, select `Data Sources` and  then `Add data source`.
    
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664777-2766ae00-1ff5-11e9-9d2f-7489f8ccbfc2.png">
    
While editing the data source, select `SimpleJson` in `Type` and `URL` is http://localhost:8888. After that, make sure IoTDB has been started, click "Save & Test", and "Data Source is working" will be shown to indicate successful configuration.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664842-554bf280-1ff5-11e9-97d2-54eebe0b2ca1.png">

#### Design in dashboard
When entering the Grafana visualization page, you can choose to add a time series, as following figure shows. You can also follow the Grafana offical documentation. For details, please refer to the Grafana official documentation. See [https://grafana.com/docs/grafana/v7.5/getting-started/](https://grafana.com/docs/grafana/v7.5/getting-started/getting-started/).

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://user-images.githubusercontent.com/13203019/51664878-6e54a380-1ff5-11e9-9718-4d0e24627fa8.png">

### config grafana

```
# ip and port of IoTDB 
spring.datasource.url=jdbc:iotdb://127.0.0.1:6667/
spring.datasource.username=root
spring.datasource.password=root
spring.datasource.driver-class-name=org.apache.iotdb.jdbc.IoTDBDriver
server.port=8888
# Use this value to set timestamp precision as "ms", "us" or "ns", which must to be same with the timestamp
# precision of Apache IoTDB engine.
timestamp_precision=ms

# Use this value to set down sampling true/false
isDownSampling=true
# defaut sampling intervals
interval=1m
# aggregation function to use to downsampling the data (int, long, float, double)
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MAX_VALUE, AVG, MIN_TIME, MIN_VALUE, NOW, SUM
continuous_data_function=AVG
# aggregation function to use to downsampling the data (boolean, string)
# COUNT, FIRST_VALUE, LAST_VALUE, MAX_TIME, MIN_TIME, NOW
discrete_data_function=LAST_VALUE
```

The specific configuration information of interval is as follows:

<1h: no sampling

1h~1d : intervals = 1m

1d~30d:intervals = 1h

\>30d：intervals = 1d

After configuration, please re-run war package

```
java -jar iotdb-grafana-{version}.war
```

