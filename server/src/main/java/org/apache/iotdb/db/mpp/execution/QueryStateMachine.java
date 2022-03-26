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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.sql.planner.plan.FragmentInstance;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * State machine for a QueryExecution. It stores the states for the QueryExecution. Others can
 * register listeners when the state changes of the QueryExecution.
 */
public class QueryStateMachine {
    private String name;
    private StateMachine<QueryState> queryState;
    private Map<FragmentInstanceId, FragmentInstanceState> fragInstanceStateMap;

    // The executor will be used in all the state machines belonged to this query.
    private Executor stateMachineExecutor;

    public QueryStateMachine(QueryId queryId) {
        this.name = String.format("QueryStateMachine[%s]", queryId);
        this.stateMachineExecutor = getStateMachineExecutor();
        this.fragInstanceStateMap = new ConcurrentHashMap<>();
        this.queryState = new StateMachine<>(queryId.toString(), this.stateMachineExecutor ,QueryState.QUEUED, QueryState.TERMINAL_INSTANCE_STATES);
    }

    public void initialFragInstanceState(FragmentInstanceId id, FragmentInstanceState state) {
        this.fragInstanceStateMap.put(id, state);
    }

    public void updateFragInstanceState(FragmentInstanceId id, FragmentInstanceState state) {
        this.fragInstanceStateMap.put(id, state);
    }

    // TODO: (xingtanzjr) consider more suitable method for executor initialization
    private Executor getStateMachineExecutor() {
        return IoTDBThreadPoolFactory.newSingleThreadExecutor(name);
    }

    public void addStateChangeListener(StateMachine.StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    private String getName() {
        return name;
    }

    public void transitionToPlanned() {
        queryState.set(QueryState.PLANNED);
    }

    public void transitionToDispatching() {
        queryState.set(QueryState.DISPATCHING);
    }

    public void transitionToRunning() {
        queryState.set(QueryState.RUNNING);
    }

    public void transitionToFinished() {
        queryState.set(QueryState.FINISHED);
    }

    public void transitionToCanceled() {
        queryState.set(QueryState.CANCELED);
    }

    public void transitionToAborted() {
        queryState.set(QueryState.ABORTED);
    }

    public void transitionToFailed() {
        queryState.set(QueryState.FAILED);
    }
}
