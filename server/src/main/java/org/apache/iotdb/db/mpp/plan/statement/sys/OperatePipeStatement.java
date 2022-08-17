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

package org.apache.iotdb.db.mpp.plan.statement.sys;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;

import java.util.Map;
import java.util.List;
import java.util.Collections;

public class OperatePipeStatement extends Statement implements IConfigStatement{

    private final OperatePipeStatement.PipeOperateType pipeOperateType;

    private String pipeName;
    private String pipeSinkName;
    private long startTime;
    private Map<String, String> pipeAttributes;

    public OperatePipeStatement(OperatePipeStatement.PipeOperateType type) {
        super();
        this.pipeName = pipeName;
        this.pipeSinkName = pipeSinkName;
        this.startTime = 0;

        pipeOperateType = type;
        switch(pipeOperateType){
            case CREATE_PIPE:
                this.setType(StatementType.CREATE_PIPE);
                break;
            case START_PIPE:
                this.setType(StatementType.START_PIPE);
                break;
            case STOP_PIPE:
                this.setType(StatementType.STOP_PIPE);
                break;
            case DROP_PIPE:
                this.setType(StatementType.DROP_PIPE);
                break;
            default:
        }
    }

    public PipeOperateType getPipeOperateType() {
        return pipeOperateType;
    }

    public String getPipeName() {
        return pipeName;
    }

    public String getPipeSinkName() {
        return pipeSinkName;
    }

    public long getStartTime() {
        return startTime;
    }

    public Map<String, String> getPipeAttributes() {
        return pipeAttributes;
    }

    public void setPipeName(String pipeName) {
        this.pipeName = pipeName;
    }

    public void setPipeSinkName(String pipeSinkName) {
        this.pipeSinkName = pipeSinkName;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setPipeAttributes(Map<String, String> pipeAttributes) {
        this.pipeAttributes = pipeAttributes;
    }

    @Override
    public QueryType getQueryType() {
        QueryType queryType;
        switch (pipeOperateType) {
            case CREATE_PIPE:
            case START_PIPE:
            case STOP_PIPE:
            case DROP_PIPE:
                queryType = QueryType.WRITE;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator: " + pipeOperateType);
        }
        return queryType;
    }

    @Override
    public List<PartialPath> getPaths() {
        return Collections.emptyList();
    }

    public enum PipeOperateType {
        CREATE_PIPE,
        START_PIPE,
        STOP_PIPE,
        DROP_PIPE
    }
}
