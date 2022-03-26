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

package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.tree.StatementVisitor;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class InsertRowStatement extends InsertBaseStatement {
    private static final Logger logger = LoggerFactory.getLogger(InsertRowStatement.class);

    private long time;
    private Object[] values;
    private boolean isNeedInferType = false;

    public InsertRowStatement() {
        super();
        statementType = StatementType.INSERT;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }

    public boolean isNeedInferType() {
        return isNeedInferType;
    }

    public void setNeedInferType(boolean needInferType) {
        isNeedInferType = needInferType;
    }

    public boolean checkDataType(Map<String, MeasurementSchema> schemaMap){
        return false;
    }

    public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
        return visitor.visitInsertRow(this, context);
    }
}
