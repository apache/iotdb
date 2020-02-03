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

Documents of Apache IoTDB (incubating) are open source. If you have found any mistakes and would like to contribute, here is a brief step:

## Contributing by Documentation Changes

### Fork and open pull Request

1. Fork the Github repository at https://github.com/apache/incubator-iotdb if you haven’t done already.
2. Clone your fork, create a new branch, push commits to the branch.
3. Open a pull request against the master branch of IoTDB. (Only in special cases would the PR be opened against other branches.) Please state that the contribution is your original work and that you license the work to the project under the project’s open source license.

### Documentation Changes

To propose a change to release documentation (that is, docs that appear under <https://iotdb.apache.org/#/Documents/progress/chap1/sec1>), edit the Markdown source files in IoTDB’s docs/ directory(`documentation-EN` branch). The process to propose a doc change is otherwise the same as the process for proposing code changes below.  

Whenever updating **User Guide** documents, remember to update `0-Content.md` at the same time. Here are two brief examples to show how to add new documents or how to modify existing documents:

1. Suppose we have "chapter 1:Overview" already, and want to add a new document `A.md` in chapter 1.
Then,
   * Step 1: add document named `5-A.md` in folder "1-Overview", since it is the fifth section in this chapter;
   * Step 2: modify `0-Content.md` file by adding `* 5-A.md` in the list of "# Chapter 1: Overview".

2. Suppose we want to create a new chapter "chapter7: RoadMap", and want to add a new document `B.md` in chapter 7.
Then,
   * Step 1: create a new folder named "7-RoadMap", and add document named `1-B.md` in folder "7-RoadMap";
   * Step 2: modify `0-Content.md` file by adding "# Chapter 7: RoadMap" in the end, and  adding `* 1-B.md` in the list of this new chapter.

If you need to insert **figures** into documents, you can firstly update the figures in [this issue](https://github.com/thulab/iotdb/issues/543) for storing pictures in IoTDB website or other MD files.
Drag a picture and then quote the figure's URL link. 

> If you want to contribute more (for example, reviewing Changes, reporting bugs, or even being commiters), please refer to [this page](/#/Development/Contributing).