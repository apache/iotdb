/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.execution;

import org.apache.iotdb.db.mpp.plan.FragmentInstance;

import java.util.List;

/**
 * QueryScheduler is used to dispatch the fragment instances of a query to target nodes. And it will
 * continue to collect and monitor the query execution before the query is finished.
 *
 * <p>Later, we can add more control logic for a QueryExecution such as retry, kill and so on by
 * this scheduler.
 */
public class QueryScheduler {
  // The stateMachine of the QueryExecution owned by this QueryScheduler
  private QueryStateMachine stateMachine;

  // The fragment instances which should be sent to corresponding Nodes.
  private List<FragmentInstance> instances;

  public QueryScheduler(QueryStateMachine stateMachine, List<FragmentInstance> instances) {
    this.stateMachine = stateMachine;
    this.instances = instances;
  }

  public void start() {}

  // Send the instances to other nodes
  private void sendFragmentInstances() {}

  // After sending, start to collect the states of these fragment instances
  private void startMonitorInstances() {}
}
