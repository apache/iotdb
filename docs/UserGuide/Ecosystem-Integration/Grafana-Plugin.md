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

## Grafana-Plugin


Grafana is an open source volume metrics monitoring and visualization tool, which can be used to present time series data and analyze application runtime status.

We developed the Grafana-Plugin for IoTDB, using the IoTDB REST service to present time series data and providing many visualization methods for time series data.
Compared with previous IoTDB-Grafana-Connector, current Grafana-Plugin performs more efficiently and supports more query types. So, **we recommend using Grafana-Plugin instead of IoTDB-Grafana-Connector**.

### Installation and deployment

#### Install Grafana 

* Download url: https://grafana.com/grafana/download
* Version >= 9.3.0


#### Acquisition method of grafana plugin

##### Method 1: grafana plugin binary Download

Download url：https://iotdb.apache.org/zh/Download/

##### Method 2: separate compilation of grafana plugin

We need to compile the front-end project in the IoTDB `grafana-plugin` directory and then generate the `dist` directory. The specific execution process is as follows.

Source download

* Plugin name: grafana-plugin
* Download url: https://github.com/apache/iotdb.git

Execute the following command:

```shell
git clone https://github.com/apache/iotdb.git
```

* Option 1 (compile with maven): execute following command in the `grafana-plugin` directory:

```shell
mvn install package -P compile-grafana-plugin
```

* Option 2 (compile with yarn): execute following command in the `grafana-plugin` directory:

```shell
yarn install
yarn build
go get -u github.com/grafana/grafana-plugin-sdk-go
go mod tidy
mage -v
```

When using the go get -u command, the following error may be reported. In this case, we need to execute `go env -w GOPROXY=https://goproxy.cn`, and then execute `go get -u github.com/grafana/grafana -plugin-sdk-go`

```
go get: module github.com/grafana/grafana-plugin-sdk-go: Get "https://proxy.golang.org/github.com/grafana/grafana-plugin-sdk-go/@v/list": dial tcp 142.251.42.241:443: i/o timeout
```

If compiling successful, you can see the `dist` directory , which contains the compiled Grafana-Plugin:

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana-plugin-build.png?raw=true">

##### Method 3: The distribution package of IoTDB is fully compiled

We can also obtain the front-end project of `grafana-plugin` and other IoTDB executable files by executing the **package instruction** of the IoTDB project.

Execute following command in the IoTDB root directory:

```shell
 mvn clean package -pl distribution -am -DskipTests -P compile-grafana-plugin
```

If compiling successful, you can see that the `distribution/target` directory contains the compiled Grafana-Plugin:

<img style="width:100%; max-width:333px; max-height:545px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/distribution.png?raw=true">


#### Install Grafana-Plugin

* Copy the front-end project target folder generated above to Grafana's plugin directory `${Grafana directory}\data\plugins\`。If there is no such directory, you can manually create it or start grafana and it will be created automatically. Of course, you can also modify the location of plugins. For details, please refer to the following instructions for modifying the location of Grafana's plugin directory.


* Modify Grafana configuration file: the file is in（`${Grafana directory}\conf\defaults.ini`）, and do the following modifications:

  ```ini
  allow_loading_unsigned_plugins = apache-iotdb-datasource
  ```
* Modify the location of Grafana's plugin directory: the file is in（`${Grafana directory}\conf\defaults.ini`）, and do the following modifications:
  
  ```ini
  plugins = data/plugins
  ```
* Start Grafana (restart if the Grafana service is already started)

For more details，please click [here](https://grafana.com/docs/grafana/latest/plugins/installation/)

#### Start Grafana

Start Grafana with the following command in the Grafana directory:

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

For more details，please click [here](https://grafana.com/docs/grafana/latest/installation/)



#### Configure IoTDB REST Service

* Modify `{iotdb directory}/conf/iotdb-common.properties` as following:

```properties
# Is the REST service enabled
enable_rest_service=true

# the binding port of the REST service
rest_service_port=18080
```

Start IoTDB (restart if the IoTDB service is already started)


### How to use Grafana-Plugin

#### Access Grafana dashboard

Grafana displays data in a web page dashboard. Please open your browser and visit `http://<ip>:<port>` when using it.

* IP is the IP of the server where your Grafana is located, and Port is the running port of Grafana (default 3000).

* The default login username and password are both `admin`.


#### Add IoTDB as Data Source

Click the `Settings` icon on the left, select the `Data Source` option, and then click `Add data source`.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/datasource_1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/datasource_2.png?raw=true">

Select the `Apache IoTDB` data source.

* Fill in `http://<ip>:<port>` in the `URL` field
  * ip is the host ip where your IoTDB server is located
  * port is the running port of the REST service (default 18080).
* Enter the username and password of the IoTDB server

Click `Save & Test`, and `Data source is working` will appear.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_datasource.png?raw=true">


#### Create a new Panel

Click the `Dashboards` icon on the left, and select `Manage` option.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/manage.png?raw=true">

Click the `New Dashboard` icon on the top right, and select `Add an empty panel` option.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/add-empty-panel.png?raw=true">

Grafana plugin supports SQL: Full Customized mode and SQL: Drop-down List mode, and the default mode is SQL: Full Customized mode.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana_input_style.png?raw=true">

##### SQL: Full Customized input method

Enter content in the SELECT, FROM , WHERE and CONTROL input box, where the WHERE and CONTROL input boxes are optional.

If a query involves multiple expressions, we can click `+` on the right side of the SELECT input box to add expressions in the SELECT clause, or click `+` on the right side of the FROM input box to add a path prefix:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana_input.png?raw=true">

SELECT input box: contents can be the time series suffix, function, udf, arithmetic expression, or nested expressions. You can also use the as clause to rename the result.

Here are some examples of valid SELECT content:

* `s1`
* `top_k(s1, 'k'='1') as top`
* `sin(s1) + cos(s1 + s2)` 
* `udf(s1) as "alias"`

FROM input box: contents must be the prefix path of the time series, such as `root.sg.d`.

WHERE input box: contents should be the filter condition of the query, such as `time > 0` or `s1 < 1024 and s2 > 1024`.

CONTROL input box: contents should be a special clause that controls the query type and output format.
The GROUP BY input box supports the use of grafana's global variables to obtain the current time interval changes $__from (start time), $__to (end time)

Here are some examples of valid CONTROL content:

*  `GROUP BY ([$__from, $__to), 1d)`
*  `GROUP BY ([$__from, $__to),3h,1d)`
*  `GROUP BY ([2017-11-01T00:00:00, 2017-11-07T23:00:00), 1d)`
*  `GROUP BY ([2017-11-01 00:00:00, 2017-11-07 23:00:00), 3h, 1d)`
*  `GROUP BY ([$__from, $__to), 1m) FILL (PREVIOUSUNTILLAST)`
*  `GROUP BY ([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUSUNTILLAST)`
*  `GROUP BY ([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (PREVIOUS, 1m)`
*  `GROUP BY ([2017-11-07T23:50:00, 2017-11-07T23:59:00), 1m) FILL (LINEAR, 5m, 5m)`
*  `GROUP BY ((2017-11-01T00:00:00, 2017-11-07T23:00:00], 1d), LEVEL=1`
*  `GROUP BY ([0, 20), 2ms, 3ms), LEVEL=1`


Tip: Statements like `select * from root.xx.**` are not recommended because those statements may cause OOM.

##### SQL: Drop-down List

Select a time series in the TIME-SERIES selection box, select a function in the FUNCTION option, and enter the contents in the SAMPLING INTERVAL、SLIDING STEP、LEVEL、FILL input boxes, where TIME-SERIES is a required item and the rest are non required items.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/grafana_input2.png?raw=true">

#### Support for variables and template functions

Both SQL: Full Customized and SQL: Drop-down List input methods support the variable and template functions of grafana. In the following example, raw input method is used, and aggregation is similar.

After creating a new Panel, click the Settings button in the upper right corner:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/setconf.png?raw=true">

Select `Variables`, click `Add variable`:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/addvaribles.png?raw=true">

Example 1：Enter `Name`, `Label`, and `Query`, and then click the `Update` button:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variblesinput.png?raw=true">

Apply Variables, enter the variable in the `grafana panel` and click the `save` button:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/applyvariables.png?raw=true">

Example 2: Nested use of variables:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variblesinput2.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variblesinput2-1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variblesinput2-2.png?raw=true">


Example 3: using function variables

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variablesinput3.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/UserGuide/Ecosystem-Integration/Grafana-plugin/variablesinput3-1.png?raw=true">

The Name in the above figure is the variable name and the variable name we will use in the panel in the future. Label is the display name of the variable. If it is empty, the variable of Name will be displayed. Otherwise, the name of the Label will be displayed.
There are Query, Custom, Text box, Constant, DataSource, Interval, Ad hoc filters, etc. in the Type drop-down, all of which can be used in IoTDB's Grafana Plugin
For a more detailed introduction to usage, please check the official manual (https://grafana.com/docs/grafana/latest/variables/)

In addition to the examples above, the following statements are supported:

*  `show databases`
*  `show timeseries`
*  `show child nodes`
*  `show all ttl`
*  `show latest timeseries`
*  `show devices`
*  `select xx from root.xxx limit xx 等sql 查询`

Tip: If the query field contains Boolean data, the result value will be converted to 1 by true and 0 by false.

#### Grafana alert function

This plugin supports Grafana alert function.

1. In the Grafana panel, click the `alerting` button, as shown in the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert1.png?raw=true">

2. Click `Create alert rule from this panel`, as shown in the figure below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert2.png?raw=true">

3. Set query and alarm conditions in step 1. Conditions represent query conditions, and multiple combined query conditions can be configured. As shown below:
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert3.png?raw=true">

The query condition in the figure: `min() OF A IS BELOW 0`, means that the condition will be triggered when the minimum value in the A tab is 0, click this function to change it to another function.

Tip: Queries used in alert rules cannot contain any template variables. Currently we only support AND and OR operators between conditions, which are executed serially.
For example, we have 3 conditions in the following order: Condition: B (Evaluates to: TRUE) OR Condition: C (Evaluates to: FALSE) and Condition: D (Evaluates to: TRUE) So the result will evaluate to ((True or False ) and right) = right.


4. After selecting indicators and alarm rules, click the `Preview` button to preview the data as shown in the figure below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert4.png?raw=true">

5. In step 2, specify the alert evaluation interval, and for `Evaluate every`, specify the evaluation frequency. Must be a multiple of 10 seconds. For example, 1m, 30s.
   For `Evaluate for`, specify the duration before the alert fires. As shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert5.png?raw=true">

6. In step 3, add the storage location, rule group, and other metadata associated with the rule. Where `Rule name` specifies the name of the rule. Rule names must be unique.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert6.png?raw=true">

7. In step 4, add a custom label. Add a custom label by selecting an existing key-value pair from the drop-down list, or add a new label by entering a new key or value. As shown below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert7.png?raw=true">

8. Click `Save` to save the rule or click `Save and Exit` to save the rule and return to the alerts page.

9. Commonly used alarm states include `Normal`, `Pending`, `Firing` and other states, as shown in the figure below:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert8.png?raw=true">
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/grafana9_alert9.png?raw=true">

10. We can also configure `Contact points` for alarms to receive alarm notifications. For more detailed operations, please refer to the official document (https://grafana.com/docs/grafana/latest/alerting/manage-notifications/create-contact-point/).

### More Details about Grafana

For more details about Grafana operation, please refer to the official Grafana documentation: http://docs.grafana.org/guides/getting_started/.
