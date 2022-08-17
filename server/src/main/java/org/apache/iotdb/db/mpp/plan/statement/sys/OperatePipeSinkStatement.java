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
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;

import java.util.Map;
import java.util.List;
import java.util.Collections;

public class OperatePipeSinkStatement extends Statement implements IConfigStatement {

    private final OperatePipeSinkStatement.PipeSinkOperateType pipeSinkOperateType;

    private String pipeSinkName;

    private String pipeSinkType;

    private Map<String,String> attributes;

    public OperatePipeSinkStatement(PipeSinkOperateType type) {
        super();
        pipeSinkOperateType = type;
        switch (pipeSinkOperateType) {
            case CREATE_PIPESINK:
                this.setType(StatementType.CREATE_PIPESINK);
                break;
            case DROP_PIPESINK:
                this.setType(StatementType.DROP_PIPESINK);
                break;
            default:;
        }
    }

    public String getPipeSinkName() {
        return pipeSinkName;
    }
    public Map<String, String> getAttributes() {
        return attributes;
    }
    public String getPipeSinkType() {
        return pipeSinkType;
    }
    public PipeSinkOperateType getPipeSinkOperateType() {
        return pipeSinkOperateType;
    }

    public void setPipeSinkName(String pipeSinkName) {
        this.pipeSinkName = pipeSinkName;
    }

    public void setPipeSinkType(String pipeSinkType) {
        this.pipeSinkType = pipeSinkType;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }


    @Override
    public QueryType getQueryType() {
        QueryType queryType;
        switch (pipeSinkOperateType) {
            case CREATE_PIPESINK:
            case DROP_PIPESINK:
                queryType = QueryType.WRITE;
                break;
            default:
                throw new IllegalArgumentException("Unknown operator: " + pipeSinkOperateType);
        }
        return queryType;
    }

    @Override
    public List<PartialPath> getPaths() {
        return Collections.emptyList();
    }

    public enum PipeSinkOperateType {
        CREATE_PIPESINK,
        DROP_PIPESINK
    }
}
