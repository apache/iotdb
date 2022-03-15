/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.mpp.operator;

import org.apache.iotdb.db.query.mpp.plan.node.PlanNodeId;

/**
 * Contains information about {@link Operator} execution.
 * <p>
 * Not thread-safe.
 */
public class OperatorContext {

    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final String operatorType;

    public OperatorContext(int operatorId, PlanNodeId planNodeId, String operatorType) {
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.operatorType = operatorType;
    }
}
