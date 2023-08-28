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

# Overview

The Consensus Package provides the Consensus Layer's service definitions and its implementations.

### What is Consensus Layer used for?

Generally, We maintain multiple copies of application data for the purpose of fault-tolerance and
data-integrity. There are variety of consensus algorithms to manage multiple copies of data, which
differs in levels of data consistency and performance. Each consensus algorithm may have multiple
industrial implementations. The Consensus Layer aims to hide all complications behind different
consensus algorithms and implementations, providing higher level of abstraction to the user.

# Consensus Layer

### Basic Concepts

* `IStateMachine` is the user application that manages a local copy of data.
* `Peer` is the smallest consensus unit (inside a process), which holds a `IStateMachine`
  internally.
* `ConsensusGroup` is a group of `Peer` all managing the same copy of data.
* `IConsensus` interface defines the basic functionality provided by Consensus Layer.
* `ConsensusFactory` is the only factory class exposed to other modules to create a consensus layer
  implementation.

User application can create a `ConsensusGroup` with k `Peer` to store data, i.e. that there will be
k copies of data.

When write data into a `ConsensusGroup` using `IConsensus::write`, it will be sent to the group
leader's `IStateMachine::write` . The leader makes decision about this write operation first, then
applies write-operation to the local statemachine using `IStateMachine::write`, and forward this
operation to other members' `IStateMachine::write` in the same group

### How to use

1. Define the  `IStateMachine` to manage local copy of data.
2. Define the `IConsensusRequest` to customize request format
3. Define the `DataSet` to customize the response format
4. Select a specific "IConsensus" class name and call `ConsensusFactory.getConsensusImpl()` to
   instantiate the corresponding consensus protocol

# Ratis Consensus Implementation

RatisConsensus is a multi-raft implementation of `IConsensus` protocol. It is based
on [Apache Ratis](https://ratis.apache.org/).

### 1. build and start a RatisConsensusImpl

```java
IConsensus consensusImpl =
    ConsensusFactory.getConsensusImpl(
        ConsensusFactory.RatisConsensus,
        new Endpoint(conf.getRpcAddress(), conf.getInternalPort()),
        new File(conf.getConsensusDir()),
        gid -> new ConfigRegionStateMachine())
    .orElseThrow(() ->
        new IllegalArgumentException(
        String.format(
        ConsensusFactory.CONSTRUCT_FAILED_MSG,
        ConsensusFactory.RatisConsensus)));

consensusImpl.start();
```

* `endpoint` is the communication endpoint for this consensusImpl.
* `StateMachineRegistry` Indicates that the consensus layer should generate different state machines for different gid
* `StoreageDir` specifies the location to store RaftLog. Assign a fix location so that the
  RatisConsensus knows where to recover when crashes and restarts.

### 2. assign local RatisConsensus a new Group

```java
ConsensusGroup group = new ConsensusGroup(...);
response = consensusImpl.addConsensusGroup(group.getGroupId(),group.getPeers());
```

The underling consensusImpl will initialize its states, and reaching out to other peers to elect the
raft leader.

**Notice**: this request may fail. It's caller's responsibility to retry / rollback.

### 3. change group configuration

#### (1) remove a member

suppose now the group contains peer[0,1,2], and we want to remove[1,2] from this group

```java
// the following code should be called in peer both 1 & 2
// first  use removePeer to inform the group leader of configuration change 
consensusImpl.removePeer(gid,myself);
// then use removeConsensusGroup to clean up local states and data
consensusImpl.removeConsensusGroup(gid);
```

**Notice**: either of `removePeer` or `removeConsensusGroup` may fail. It's caller's responsibility
to retry and make these two calls atomic.

#### (2) add a member

adding a new member is similar to removing a member except that you should call `addConsensusGroup`
first and then `addPeer`

```java
// the following code should be called in peer both 1 & 2
// first addConsensusGroup to initialize local states
consensusImpl.addConsensusGroup(gid);
// then use addPeer to inform the previous group members of joining a new member
consensusImpl.addPeer(gid,myself);
```

#### (3) add/remove multiple members

```java
// pre. For each member newly added, call addConsensusGroup locally to initialize
consensusImpl.changePeer(group.getGroupId(),newGroupmember);
// after. For each member removed, call removeConsensusGroup locally to clean up
```

**Notice**: the old group and the new group must overlap in at least one member.

### 4. write data

```java
ConsensusWriteResponse response = consensusImpl.write(gid,request)
if(response.isSuccess() && response.getStates().code() == 200){
    ...
}
```

### 5. read data

```java
ConsensusReadResponse response = consensusImpl.read(gid,request);
if(response.isSuccess()){
    MyDataSet result=(MyDataSet)response.getDataset();
}
```

**NOTICE**: currently in RatisConsensus, read will direct read the local copy. Thus, the result may
be stale and not linearizable!


