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

# System Integration



## Grafana Plugin

Grafana is an open source tool for monitoring metrics and visualization, which could be applied to display time series and analyze the operation of application programms.

In the IoTDB project, we developed the Grafana Plugin to display time series through the IoTDB REST service, and it has provided many visualization methods for time series. Compared to IoTDB-Grafana-Connector, the Grafana Plugin works more efficiently and supports more types of queries. As long as it's possible in your deployment environment, *we highly recommand applying the Grafana Plugin rather than IoTDB-Grafana-Connector.*




### Deployment of the Grafana Plugin

#### Install Grafana

* Download Grafana: https://grafana.com/grafana/download
* Version >= 7.0.0



#### Download Grafana-plugin

* Plugin Name: grafana-plugin
* Downloads: https://github.com/apache/iotdb.git

Execute the command below:

```shell
git clone https://github.com/apache/iotdb.git
```



#### Compile grafana-plugin

##### Option 1

We need to compile the front-end engineering under the `grafana-plugin` directory in IoTDB repository and generate the target directory `dist`. Below is the execution flow in detail:

You can use either of the following compilation methods:

* Compile with maven, execute the command under `grafana-plugin`：

```shell
mvn install package
```

* Or compile with yarn, execute the command under `grafana-plugin`：

```shell
yarn install
yarn build
```

If sucessed, we could find the generated target directory `dist`, which contains the compiled front-end Grafana Plugin:

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana-plugin-build.png?raw=true">

##### Option 2

We could also obtain the front-end engineering of `grafana-plugin ` and other supporting IoTDB executables by executing the **packaging command** of the IoTDB project.

Execute the command under the root directory of IoTDB:

```shell
 mvn clean package -pl distribution -am -DskipTests 
```

If sucessed, we could find the generated target directory `distribution/target`, which consists the compiled front-end Grafana Plugin:

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/distribution.png?raw=true">



#### Install grafana-plugin

* Copy the generated front-end engineering target folder to the Grafana plugin directory `${Grafana File Directory}\data\plugins\`
  * In Windows, the `data\plugins` directory will be automatically generated after Grafana booted
  * In Linux, the plugins directory `/var/lib/grafana/plugins` needs to be manually created
  * In MacOS，the plugins directory is `/usr/local/var/lib/grafana/plugins` (Check the CMD output after installing Grafana with 'brew install' for details of the path)

* Modify the profile of Grafana: find the profile (`${Grafana File Directory}\conf\defaults.ini`), and make the following modification:

  ```ini
  allow_loading_unsigned_plugins = iotdb
  ```

* If the Grafana service is already started, it needs to be rebooted.



#### Boot Grafana

Go to the Grafana installation directory and start Grafana with the following command:

* Windows:

```shell
bin\grafana-server.exe
```
* Linux:

```shell
sudo service grafana-server start
```
* MacOS:

```shell
brew services start grafana
```
Click [Here] for more details (https://grafana.com/docs/grafana/latest/installation/)



#### Configure the IoTDB REST Service

Go to `{iotdb Directory}/conf`, open `iotdb-rest.properties` and make the following modifications:

```properties
# Is the REST service enabled
enable_rest_service=false

# the binding port of the REST service
rest_service_port=18080
```

Start(reboot) IoTDB to bring the configuration into effect, now the IoTDB Rest service is in operation.



### Apply the Grafana Plugin

#### Visit Grafana Dashboard

Grafana displays the data for you in web dashbboards. Please visit `http://<ip>:<port>` while using Grafana.
Note：`<ip>` is the IP address of the server on which your Grafana resides, and `<port>` is the running port of Grafana (Default: 3000).
On a local trial, the default address for the Grafana Dashboard is `http://localhost:3000/`.

The default username and password are both `admin`.



#### Add IoTDB Data Source

Click the 'Settings' icon on the left, select the `Data Source` option, then click `Add data source`.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_2.png?raw=true">

Select the data source `Apache IoTDB`, fill the `URL` with `http://<ip>:<port>`.

`<ip>` is the host IP address where your IoTDB server resides, and `<port>` is the running port of the REST service  (Default: 18080).

Enter the username and password of the IoTDB server, then click `Save & Test`, the configuration is succeeded when `Success` was output.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_3.png?raw=true">



#### Create a new Panel

Click the `Dashboards` icon on the left, select the `Manage` option as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/manage.png?raw=true">

Click the `New Dashboard` icon on the upper-right and select `Add an empty panel` as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/add%20empty%20panel.png?raw=true">

Enter the content in the SELECT, FROM, WHERE, and CONTROL fields. The WHERE and CONTROL fields are optional.

If a query involves more than one expression, we could add the expression in the SELECT clause by clicking on the `+` to the right of the SELECT input box, or add the path prefix by clicking on the `+` to the right of the FROM input box, as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana_input.png?raw=true">

The contents of the SELECT input box can be suffixes of time series, functions or user-defined functions, arithmetic expressions, or nested expressions of them. You could also use the AS clause to rename the result sequence name that you want to display.

Here are some examples of valid inputs in the SELECT input box:

*  `s1`
*  `top_k(s1, 'k'='1') as top`
*  `sin(s1) + cos(s1 + s2)`
*  `udf(s1) as "Alias"`

The contents of the FROM input box must be the prefix path of the time series, for example `root.sg.d`。

The WHERE input box is optional. The content should be the filtering criteria for the query, for example `time > 0` or `s1 < 1024 and s2 > 1024`。

The CONTROL input field is optional and should contain special clauses to control the query type and output format. Here are some examples of valid inputs to the CONTROL input field:

*  `group by ([2017-11-01T00:00:00, 2017-11-07T23:00:00), 1d)`
*  `group by ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d)`
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUSUNTILLAST)`
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUS, 1m)`
*  `GROUP BY([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (LINEAR, 5m, 5m)`
*  `group by ((2017-11-01T00:00:00, 2017-11-07T23:00:00], 1d), level=1`
*  `group by ([0, 20), 2ms, 3ms), level=1`



#### Support for Variables and Templates

This plugin supports the variables and templates of Grafana (https://grafana.com/docs/grafana/v7.0/variables/).

After creating a new Panel, click the "Settings" button on the upper-right, as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/setconf.png?raw=true">

Select `Variables`, click `Add variable`, as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/addvaribles.png?raw=true">

Enter Name, Label and Query, click the `Update` button, as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput.png?raw=true">

Apply Variables, enter the variables in `grafana  panel` and click `save`, as shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/applyvariables.png?raw=true">



### More Informations

More details about how Grafana works can be found in the official documentation of Grafana: http://docs.grafana.org/guides/getting_started/

