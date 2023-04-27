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

# How to Contribute Code
## Process
Tasks are managed as issues in JIRA.
The full lifecycle of an Issue: Create an issue -> assign an issue -> submit a pr(pull request) -> review a pr -> squash merge a pr -> close an issue.

## Contributing Conventions

 ### Creating an Issue
 There are a few things to keep in mind when creating an issue in [ JIRA ](https://issues.apache.org/JIRA/projects/IOTDB/issues)
 1. Naming: Try to make it clear and easy to understand. Examples include supporting a new aggregate query function (avg) and optimizing the performance of querying raw data . The name of the issue will later be used as the release note.

 2. Description: Issue of new features and improvements should be clear. Bug reports should cover the environment, load, phenomenon (abnormal log), the affected version(s) , etc.  And it's best to include ways to reproduce the bug. 

 ### Assigning an Issue
 When assigning an issue in JIRA for yourself, it's recommended to add the comment, "I'm doing this", otherwise there might be duplication of effort.
Note: If you can't assign an issue, it is because your account doesn't have the necessary permission.
To address this, please send an email to the dev@iotdb.apache.org mailing list with the title of  [application] apply for permission to assign issues to XXX (your JIRA username).。
### Submitting a PR
#### What you need to submit
Issue type : New Feature

1.Submit the user manual and the pr for code changes. 

A user manual is mainly for helping users understand how the functions work and how to use them. It is recommended to contain scenario and background, configuration, interface description and examples. The user manual of the official website is placed in the docs/UserGuide folder of apache/iotdb repository. To update the user manual directory, including adding, deleting documents and renaming documents, you need to make corresponding changes in the file(path:site/src/main/.vuepress/config.js) in the master branch.

2.Submit UT (unit test) or IT (integration test). 


When adding unit tests or integration tests , try to cover as many cases as possible.  xxTest(path: iotdb/server/src/test/java/org/apache/iotdb/db/query/aggregation/) and xxIT(path: iotdb/integration/src/test/java/org/apache/iotdb/db/integration/) can be used as reference.

Issue type : Improvement

1.Submit the code and UT(if importing new scenario)
2.etter to submit test results, including quantified improvements and possible negative effects.  

Issue type : Bug

Submit  UT or IT that can reproduce the bug.  

#### Coding Reminders
Branch management

The IoTDB version naming method is 0.{major version}.{minor version}. For example, for version 0.12.4, 12 is the major version and 4 is the minor version. 

As the current development branch, the master branch corresponds to the next major release version. When each major version is released for the first time, a separate branch will be created for archiving. For example, codes of  the 0.12.x versions are placed under the rel/0.12 branch.

If a bug of a released version is found and fixed, the bugfix pr should be submitted to all branches that are newer than the specific branch. For example, a pr which is about a version 0.11.x bugfix should be submitted to rel/0.11 branch, rel/0.12 branch and master branch.

Code formatting
It is required to use "mvn spotless:apply" to format the code before committing, otherwise, the ci code format check will fail. 

Notes

1.The default values need to be consistent between iotdb-datanode.properties file and IoTDBConfig file.

2.To modify the configuration parameters, the following files need to be modified 

a.Configuration file: server/src/assembly/resources/conf/iotdb-datanode.properties

b. Codes: IoTDBDescriptor, IoTDBConfig

c. Documentation: docs/UserGuide/Reference/DataNode-Config-Manual.md

3.To modify configuration parameters in IT and UT, you need to modify them in the method annotated by @before and reset them in the method annotated by @after. In this way, you can avoid impact on other tests. The parameters of the compaction module are placed in the CompactionConfigRestorer file.

#### PR Naming
Format: [To branch] [Jira number] PR name

Example: [To rel/0.12] [IoTDB-1907] implement customized sync process: sender

To branch

It is required when submitting pr to a non-master branch (such as rel/0.13, in which case the pr name should contain [To rel/0.13]) and not required when submitting to a master branch.

Jira number

The name should start with a JIRA number so that the PR can be automatically linked to the corresponding issue. Example includes [IOTDB-1907] implement customized sync process: sender.
This auto-linking won't happen if the PR is created without any JIRA number or with one that is improper, in which case you need to correct the PR name and manually paste the PR link to the issue page by adding a comment or attaching a link.

#### PR Description
Usually, the PR name can't reflect all changes, so it is better to add a description about what has been changed and give explanations for any difficult-to-understand part.

The description of a bug-fixing pr needs to cover the cause of the bug and how to fix it, as well as the added UT/IT test cases and associated negative effects.

#### After Submitting a PR

Send to the dev@iotdb.apache.org mailing list an email that describes the PR in detail, then carefully read and respond to all review comments, and make changes after reaching a consensus. 

### Reviewing a PR
Checklist
1. Is the PR named correctly? and whether any new feature and bug fix have an issue number.
2. Is PR description sufficiently clear? 
3. Are UT (unit test) or performance test reports submitted?
4. Is the user manual of the new feature submitted?
5. It should not contain code modifications for other issues. Irrelevant modifications should be placed in other PR. 

How to review a pr

1. Click Files changed 
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B1.png?raw=true">

2. Add review comments. First, move your mouse to the left. And then there will be a plus sign, click the plus sign. Second, write comments. Third, click Start a Review. At this step, all review comments will be temporarily saved, which others can not see.<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B2.png?raw=true">
3. Submit review. After all the comments are added, click Review Changes and select your opinion. Select "Approve" for those that can be combined. Select  "Request Changes" or "Comment" for those that need to be modified. Select  "Comment" for those that are not sure. Finally, submit a review and only the person submitting the PR can see the review.
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://github.com/apache/iotdb-bin-resources/blob/main/docs/zh/development/howtocontributecode/%E4%BB%A3%E7%A0%81%E5%AE%A1%E9%98%85%E6%B5%81%E7%A8%8B3.png?raw=true">

### Merging a PR
Make sure that all review comments are responded. 

Obtain approval from at least 1 committer.  

Choose squash merge. You can choose rebase only when the author has only one commit record with a clear commit log.

Close the corresponding issue in JIRA, and mark the repaired or completed version. Note that solving or closing an issue requires adding a pr or description to the issue, so that changes can be tracked via the issue.

## How to Prepare User Manual and Design Document
User manual and other documentation on the official website are maintained in the apache/iotdb repository. 

The index items of each page of the official website are maintained in the file "site/src/main/.vuepress/config.js" of the master branch, while the specific content of the user manual is maintained in the branch of the corresponding version (for example, user manual for 0.12 is in rel/0.12).  

User manual

It is mainly for helping users understand how the functions work and how to use them.
It is recommended that the user manual contains scenario and background, configuration, interface description and examples.。

Design document

It is mainly for helping developers understand how to implement a function, including the organization of code modules and algorithms.
It is recommended that the design document contains background, design goals, idea, main module, interface design. 

### Modifying User Manual
Other than modifying different files, the process is the same as contributing codes.

The English user manual is placed in the docs/UserGuide folder.

To update the user manual directory, including adding, deleting documents and renaming documents, you need to make corresponding changes in the file(path:site/src/main/.vuepress/config.js) in the master branch.

### Modifying the Top Navigation Bar of the Official Website

Search for nav in the file(path:site/src/main/.vuepress/config.js), and make corresponding modifications by referencing the existing code, then submit a PR and wait for merging. Documents to be added can be placed in the docs folder.