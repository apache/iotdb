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


### How to use Grafana-Plugin

#### Access Grafana dashboard

Grafana displays data in a web page dashboard. Please open your browser and visit `http://<ip>:<port>` when using it.

* IP is the IP of the server where your Grafana is located, and Port is the running port of Grafana (default 3000).

* The default login username and password are both `admin`.


#### Add IoTDB as Data Source

Click the `Settings` icon on the left, select the `Data Source` option, and then click `Add data source`.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_2.png?raw=true">

Select the `Apache IoTDB` data source.

* Fill in `http://<ip>:<port>` in the `URL` field
   * ip is the host ip where your IoTDB server is located
   * port is the running port of the REST service (default 18080).
* Enter the username and password of the IoTDB server

Click `Save & Test`, and `Success` will appear.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/datasource_3.png?raw=true">


#### Create a new Panel

Click the `Dashboards` icon on the left, and select `Manage` option.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/manage.png?raw=true">

Click the `New Dashboard` icon on the top right, and select `Add an empty panel` option.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/add%20empty%20panel.png?raw=true">

Grafana plugin supports SQL: Full Customized mode and SQL: Drop-down List mode, and the default mode is SQL: Full Customized mode.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana_input_style.png?raw=true">

##### SQL: Full Customized input method

Enter content in the SELECT, FROM , WHERE and CONTROL input box, where the WHERE and CONTROL input boxes are optional.

If a query involves multiple expressions, we can click `+` on the right side of the SELECT input box to add expressions in the SELECT clause, or click `+` on the right side of the FROM input box to add a path prefix:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana_input.png?raw=true">

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

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/grafana_input2.png?raw=true">

#### Support for variables and template functions

Both SQL: Full Customized and SQL: Drop-down List input methods support the variable and template functions of grafana. In the following example, raw input method is used, and aggregation is similar.

After creating a new Panel, click the Settings button in the upper right corner:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/setconf.png?raw=true">

Select `Variables`, click `Add variable`:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/addvaribles.png?raw=true">

Example 1：Enter `Name`, `Label`, and `Query`, and then click the `Update` button:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput.png?raw=true">

Apply Variables, enter the variable in the `grafana panel` and click the `save` button:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/applyvariables.png?raw=true">

Example 2: Nested use of variables:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput2.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput2-1.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variblesinput2-2.png?raw=true">


Example 3: using function variables

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variablesinput3.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/variablesinput3-1.png?raw=true">

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

1. In the Grafana sidebar, hover over the `Alerting` icon and click `Notification channels`.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alerting1.png?raw=true">

2. Click Add Channel.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alerting2.png?raw=true">

3. Fill in the fields described below or select options. There are many types of Type, including DingDing, Email, Slack, WebHook, Prometheus Alertmanager, etc.
   This sample Type uses `Prometheus Alertmanager`. Prometheus Alertmanager needs to be installed in advance. For more detailed configuration and parameter introduction, please refer to the official documentation: https://grafana.com/docs/grafana/v8.0/alerting/old- alerting/notifications/.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alerting3.png?raw=true">

4. Click the `Test` button, the `Test notification sent` appears, click the `Save` button to save

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alerting4.png?raw=true">

5. After creating a new Panel, enter the query parameters and click Save, then select `Alert` and click `Create Alert`, as shown in the following figure:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertpanle1.png?raw=true">

6、Fill out the fields described below or select an option， `Name`- Enter a descriptive name. The name will be displayed in the Alert Rules list. This field supports templating.
`Evaluate every` - Specify how often the scheduler should evaluate the alert rule. This is referred to as the evaluation interval.
`For` - Specify how long the query needs to violate the configured thresholds before the alert notification triggers.。`Conditions`- Represents query criteria. Multiple combined query criteria can be configured.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertpanle2.jpg?raw=true">

Query conditions in the figure：avg() OF query(A,5m,now) IS ABOVE -1

avg() Controls how the values for each series should be reduced to a value that can be compared against the threshold. Click on the function to change it to another aggregation function
query(A, 15m, now) The letter defines what query to execute from the Metrics tab. The second two parameters define the time range, 15m, now means 15 minutes ago to now. You can also do 10m
IS ABOVE -1 Defines the type of threshold and the threshold value. You can click on  IS ABOVE to change the type of threshold

Tips:The query used in an alert rule cannot contain any template variables. Currently we only support AND and OR operators between conditions and they are executed serially.

For example, we have 3 conditions in the following order: condition:A(evaluates to: TRUE) OR condition:B(evaluates to: FALSE) AND condition:C(evaluates to: TRUE) so the result will be calculated as ((TRUE OR FALSE) AND TRUE) = TRUE.

More details can be found in the official documents:https://grafana.com/docs/grafana/latest/alerting/old-alerting/create-alerts/


7、Click the `Test rule` button and the `firing: true` appears, the configuration is successful, click the `save` button

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertpanel3.png?raw=true">

8、The following figure shows the alarm displayed in the grafana panel

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertpanel4.png?raw=true">

9、View alert rules

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertPanel5.png?raw=true">

10、View alert records in promehthus alertmanager

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/UserGuide/Ecosystem%20Integration/Grafana-plugin/alertpanel6.png?raw=true">


### More Details about Grafana

For more details about Grafana operation, please refer to the official Grafana documentation: http://docs.grafana.org/guides/getting_started/.
