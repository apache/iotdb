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
# Common issues making the build fail

## General Problems

When building some modules on more modern Java version, I keep on getting these messages, and then the JVM pauses for some time, causing quite a delay in the execution of the tests.

    OpenJDK 64-Bit Server VM warning: CodeCache is full. Compiler has been disabled.
    OpenJDK 64-Bit Server VM warning: Try increasing the code cache size using -XX:ReservedCodeCacheSize=

## iotdb-core/node-commons

If a test failed, there seem to be zombie processes hanging around blocking access to the port the test needs. So in order to get the build to work I always need to run:

    killall -9 java

## Flaky tests

### IoTDB: Core: Consensus

    RatisConsensusTest.addMemberToGroup:132 expected:<3> but was:<2>

## Failing tests

### IoTDB: Core: Data-Node (Server)

Might have something to do with daylight-saving time as the difference is pretty much one hour

    [ERROR]   TimeRangeIteratorTest.testMixedUnit:366->checkRes:437 expected:<...77625200000 : 168038[63]99999 ]> but was:<...77625200000 : 168038[99]99999 ]>
