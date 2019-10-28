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

# Chapter 9: System Tools

## LogVisualizer

<!-- TOC -->

- [LogVisualizer](#LogVisualizer)
- [Introduction](#introduction)
- [Preparation](#preparation)
- [Usage(GUI)](#Usage(GUI))
  - [Run the gui](#Run the gui)
  - [Choose a log file](#Choose the log file(s))
  - [Choose a parser property file](#Choose a parser property file)
  - [Load logs](#Load logs)
  - [Create new plans](#Create new plans)
  - [Edit plans](#Edit plans)
  - [Delete plans](#Delete plans)
  - [Execute plans](#Execute plans)
- [Usage(CommandLine)](#Usage(CommandLine))
  

<!-- /TOC -->

# Introduction
IoTDB generates a variety of logs during runtime like other software systems. The debug and trace
logs can help the developers keep trace of the status of IoTDB and dig out potential or unclear bugs
The info logs may tell how healthy the system is and point out directions for database parameter
optimization. The warn and error logs indicate the system is in a dangerous status or 
something unexpected happened (very likely a bug or an attack) and helps the database 
administrator save the system before it crashes or find the reasons why it has crashed.

With the logs being so valuable a source of monitoring system status, mining the logs for their 
potential precious information has become quite a hot issue both in academy and industry. 
Traditional ways often require much human-resource to discover the value behind the logs by 
reading and analyzing the logs carefully. Usually, the logs of different sources (like Java 
classes) are mixed together in several files and only the most experienced debugging experts know 
from the beginning what logs exactly they long for, many less experienced programmers would spend 
time reading unrelated logs and filtering them out. Nevertheless, logs are mostly composed with 
strings containing numbers which describe the system status and they are much less straight-forward for 
humans to understand and compare than plots and statistics.
 
To reduce the difficulty of understanding what has happened behind these logs and better comparing
them, we present a simple tool LogVisualizer to retrieve interesting (specified by the user) 
variables in the log files, generate timeseries based on these observations and draw plots to show 
them in a more human-friendly way. LogVisualizer also collects the statistics of the logs to 
present an overall view of what was going on.

# Preparation
Before using LogVisualizer, you should have it built. Since LogVisualizer is integrated in the 
server module as a tool, you can build the server together with LogVisualizer using:
> mvn clean package -pl server -DskipTests

Then you fan find the scripts to start LogVisualizer in: 
`server/target/iotdb-server-{project-version}-SNAPSHOT/tools/logVisualize`

To have access to the advanced usages of this tool, you must have some basic knowledge about regular
expressions, especially Java style ones. Knowing about regular expressions enables to create your own
visualization plans or modify the existing ones. However, if you are totally new to regular expressions,
you can still use the preset plans which are only applicable to IoTDB.

# Usage(GUI)
## Run the gui
You can run LogVisualizer using `log-visualizer-gui.sh` (or `log-visualizer-gui.bat` in windows). 
This will bring you a gui like:

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/main_panel.png?raw=true">

## Choose the log file(s)
First, you should choose a file containing the logs (or a directory containing such files) to be 
visualized. Notice that we currently only support single-line logs and logs with multiple lines 
will be ignored.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/select_log_file.png?raw=true">

## Choose a parser property file
Second, you should also choose a file describing how to parse the log file. For log files 
produced by IoTDB, we provide an example in 
`server/src/assembly/resources/tools/logVisualize/default.log.pattern`. If your logs have 
similar structure as the IoTDB ones, you may just use it, otherwise, you should read that file 
carefully and modify it for your logs.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/select_log_parser_property.png?raw=true">

## Load logs
Then, click the `Load logs` button to let the LogVisualizer to prepare to read the logs. If the 
preparation is successful, the label above will show `Logs are successfully loaded`, otherwise, a
dialog will show you what has gone wrong and you should fix it accordingly.
 
 <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/load_logs.png?raw=true">
 
## Load plans
Next, you can load existing visualization plans or create new ones. For loading existing plans, 
just click the button `Load plans` and choose the log files or directories that contain log 
files. Notice that if choose a directory, then the directory must only contain visualization plans.

 <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/load_plan.png?raw=true">
 
## Create new plans
Now, we will present an example of how to create a new plan. First, click the `Create plan` 
button and choose a place to save the new plan, then the plan will show up in the list in the 
left. The name of the file will be used as the plan's name and it must be different from 
any existing plans.

 <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/create_plan.png?raw=true">

## Edit plans
After creating the new plan, you can edit it in the right panel. We provide an example in 
 `server/src/assembly/resources/tools/logVisualize/plans/flushTimeConsumption.plan.example`, 
we strongly recommend you read it carefully to know how to fill the fields. You can also edit plans 
you have loaded here. Do not forget to click the `Save plan` button if you want to save what you 
have modified. If you want to create a plan that shows the time consumptions of jobs grouped by 
their type, you may set the fields like the following one.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/edit_plan.png?raw=true">

## Delete plans
If you don't need a plan anymore, you can select it and click `Delete plan` button. This will 
remove the plan from the disk.

## Execute plans
Finally, you can execute a plan by selecting it and click `Execute plan`. Please make sure you 
have loaded the logs. Afterwards, two tabs will be added into the main panel. The first contains 
timeseries charts each corresponding to log groups and plotting the measurements specified by the 
plan. The second one presents the statistics of each measurement of each log group. Once you have 
done browsing, you can click the button on the up-right to close the tab.

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/execute_plan.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/plot.png?raw=true">

<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/jt2594838/PicHub/blob/master/log-visualizer/statistics.png?raw=true">
 
# Usage(CommandLine)
 We also provide a command line tool of LogVisualizer, which is `log-visualizer-cmd.sh` 
 (or `log-visualizer-cmd.bat`). This tool takes exactly 4 parameters, which are: `the path of the log file` 
 `the path of the parser property file` `the path of the visualization plan(s)` `the output directory`.
 Running this tool will load the given logs, run all the plans and save the result of each plan into
 a sub-directory of the output path.
 E.g.: 
 > ./log-visualizer-cmd.sh all.log default.log.pattern planB.plan results

