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

# IoTDB Working Process

## Main link

IoTDB official website：https://iotdb.apache.org/

Code library：https://github.com/apache/iotdb

Code library for go language: https://github.com/apache/iotdb-client-go

Library for resources (project's documents, compiler, etc): https://github.com/apache/iotdb-bin-resources

Get started quickly：http://iotdb.apache.org/UserGuide/Master/QuickStart/QuickStart.html

Jira Task Management：https://issues.apache.org/jira/projects/IOTDB/issues

Wiki Document Management：https://cwiki.apache.org/confluence/display/IOTDB/Home\

Mailing list: https://lists.apache.org/list.html?dev@iotdb.apache.org

Everyday build: https://ci-builds.apache.org/job/IoTDB/job/IoTDB-Pipe/job/master/

Slack: https://apacheiotdb.slack.com/join/shared_invite/zt-qvso1nj8-7715TpySZtZqmyG5qXQwpg#/shared-invite/email

## Subscribe to mailing list

The mailing list is where the Apache project conducts technical discussions and communication with users. Follow the mailing list to receive mail.

Mailing list address：dev@iotdb.apache.org

Follow method: Send an email to dev-subscribe@iotdb.apache.org with the email you want to receive the email, the subject content is not limited, after receiving the reply, send a confirmation email to the confirmation address again (the confirmation address is longer, it is recommended  gmail mailbox).


Other mailing list:
* notifications@iotdb.apache.org (for JIRA information notification.)
  * If you only want to receive individual Jira notifications that you are interest in, you do not need to subscribe this mailing list. Instead, you just need to click "start-watching this issue" on the jira issue webpage or just comment on this issue.
* commits@iotdb.apache.org (Any code changes will be notified here, please note that this mailing list will be very large)
* reviews@iotdb.apache.org (Any code review comments will be notified here, please note that this mailing list will be very large)



## New features, bug feedback, improvements and more

All features or bugs that you want IoTDB to do can be raised on Jira：https://issues.apache.org/jira/projects/IOTDB/issues

You can choose issue types: bug, improvement, new feature, etc.  New issues will be automatically synchronized to the mailing list (notifications@), and subsequent discussions can be left on jira or on the mailing list. When the issue is resolved, close the issue.

## Email discussion content (English)

* Joining the mailing list for the first time can introduce youself briefly.  (Hi, I'm xxx ...)

* Before developing a new feature, you can send an e-mail to declare the task you want to do.（Hi，I'm working on issue IOTDB-XXX，My plan is ...）

## Contributing documents

The content of all IoTDB official websites is in the docs of the project root directory:

* docs/SystemDesign: System Design Document-English Version
* docs/zh/SystemDesign: System Design Document-Chinese Version
* docs/UserGuide: User Guide English Version
* docs/zh/UserGuide: User Guide Chinese Version
* docs/Community: community English Version
* docs/zh/Community: community Chinese Version
* docs/Development: Development Guide English Version
* docs/zh/Development: Development Guide Chinese Version

Correspondence between versions and branches on the official website:

* In progress -> master
* major_version.x -> rel/major_version （eg 0.9.x -> rel/0.9）

Precautions:

* Images in Markdown can be uploaded to https://github.com/apache/iotdb-bin-resources for url
* Do not use special Unicode chars, e.g., U+FF1A 
* Do not use the character of dollar (as we will use Latex to generate pdf files)

## Code Formatting

We use the [Spotless
plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) together with [google-java-format](https://github.com/google/google-java-format) to format our Java code. You can configure your IDE to automatically apply formatting on saving with these steps(Take idea as an example):

1. Download the  [google-java-format
   plugin v1.7.0.5](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/83169), it can be installed in IDEA (Preferences -> plugins -> search google-java-format), [More detailed setup manual](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides)
2. Install the plugin from disk (Plugins -> little gear icon -> "Install plugin from disk" -> Navigate to downloaded zip file)
3. In the plugin settings, enable the plugin and keep the default Google code style (2-space indents)
4. Remember to never update this plugin to a later version，until Spotless was upgraded to version 1.8+.
5. Install the [Save Actions
   plugin](https://plugins.jetbrains.com/plugin/7642-save-actions) , and enable the plugin, along with "Optimize imports" and "Reformat file"
6. In the "Save Actions" settings page, setup a "File Path Inclusion" for `.*\.java`. Otherwise you will get unintended reformatting in other files you edit.
7. Fix the issues of reordering the import packages: in IDEA: choose: Preferences | Editor | Code Style | Java | imports. At the tail of the panel, there is "Import Layout", change it to:
```shell
   import org.apache.iotdb.*
   <blank line>
   import all other imports
   <blank line>
   import java.*
   <blank line>
   import static all other imports
```
8. Before you submit codes, you can use `mvn spotless:check` to check your codes manually,
and use `mvn spotless:apply` to format your codes.

## Code Sytle
We use the [maven-checkstyle-plugin](https://checkstyle.sourceforge.io/config_filefilters.html) to make Java codes obey a consistent ruleset defined in [checkstyle.xml](https://github.com/apache/iotdb/blob/master/checkstyle.xml) under the project root.

You can reference the code style rules in the file. After development, you can use `mvn validate` to check whether your codes satisfy the style rules.

Besides, when you develop in an IDE, some default code style configurations may be conflict with those rules.

In IDEA, you can follow these steps to change those inconsistent style formatting rules:

### Disable using wildcard import

1. Navigate to Java code style configuration page (Preferences... -> Editor -> Code Style -> Java).
2. Switch to the 'imports' tab.
3. In the 'General' section, enable 'Use single class import' option.
4. Change 'Class count to use import with '\*'' to 999 or another very large number.
5. Change 'Names to count to use static import with '\*'' to 999 or another very large number.

## Contributing code

You can go to jira to pick up the existing issue or create your own issue and get it. The comment says that I can do this issue.

* Clone the repository to your own local repository, clone to the local, and associate the apache repository as the upstream upstream repository.
* Cut out a new branch from master. The branch name is determined by the function of this branch. It is usually called f_new_feature (such as f_storage_engine) or fix_bug (such as fix_query_cache_bug).
* Add code style as the root java-google-style.xml in the idea
* Modify the code and add test cases (unit test, integration test)
  * Integration test reference:server/src/test/java/org/apache/iotdb/db/integration/IoTDBTimeZoneIT
* Use `mvn spotless:check` to check the code style and use `mvn spotless:apply` to correct the code style
* Submit a PR, starting with [IOTDB-jira number]
* Email to dev mailing list：(I've submitted a PR for issue IOTDB-xxx [link])
* Make changes based on other people's reviews and continue to update until merged
* close jira issue

# IoTDB Debug Guide

## Import the code

### Intellij idea

It is recommended to use Intellij idea。```mvn clean package -DskipTests``` 

Mark ```antlr/target/generated-sources/antlr4``` and```thrift/target/generated-sources/thrift``` as ```Source Root```。 

### Eclipse

If it is a version before eclipse 2019，users need to be executed in the root directory of IoTDB `mvn eclipse:eclipse -DskipTests`。

import -> General -> Existing Projects into Workspace -> Select IoTDB root directory

If the version after eclipse 2019

import -> Maven -> Existing Maven Projects


# Frequent Questions When Compiling the Source Code

>Q: I could not download thrift-* tools, like `Could not get content
org.apache.maven.wagon.TransferFailedException: Transfer failed for https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-ubuntu`

 It is due to some network problems (especially in China), you can:

 * Download the file from the URL manually;
      * https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-ubuntu

      * https://github.com/apache/iotdb-bin-resources/blob/main/compile-tools/thrift-0.14-MacOS

 * Put the file to thrift/target/tools/

 * Re-run maven command like `mvn compile`

 
 >Q: IConfigNodeRPCService class is unrecognized (IDEA can not find the class even though we have generated it)

 It is because Thrift generate the file too large, which exceeds the lines that IDEA can parse by default. You can find that file and then you will see IDEA claims that. To make it work, you can:
 
 * Click "Help" menu of IDEA
 * Choose "Edit Customed Properties"
 * On the opened file (`idea.properties`), add: `idea.max.intellisense.filesize=9000`
 * Restart IDEA
 
 


## Recommended Debug Tool 

The initial committer of IoTDB is using this  [Java profiler](https://www.ej-technologies.com/products/jprofiler/overview.html) to debug. Welcome all IoTDB contributors to use it.
