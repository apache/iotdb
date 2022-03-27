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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.tree.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

public class InsertRowStatement extends InsertBaseStatement {
  private static final Logger logger = LoggerFactory.getLogger(InsertRowStatement.class);

  private static final byte TYPE_RAW_STRING = -1;
  private static final byte TYPE_NULL = -2;

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

  public void fillValues(ByteBuffer buffer) throws QueryProcessException {
    this.values = new Object[measurements.length];
    for (int i = 0; i < dataTypes.length; i++) {
      // types are not determined, the situation mainly occurs when the plan uses string values
      // and is forwarded to other nodes
      byte typeNum = (byte) ReadWriteIOUtils.read(buffer);
      if (typeNum == TYPE_RAW_STRING || typeNum == TYPE_NULL) {
        values[i] = typeNum == TYPE_RAW_STRING ? ReadWriteIOUtils.readString(buffer) : null;
        continue;
      }
      dataTypes[i] = TSDataType.values()[typeNum];
      switch (dataTypes[i]) {
        case BOOLEAN:
          values[i] = ReadWriteIOUtils.readBool(buffer);
          break;
        case INT32:
          values[i] = ReadWriteIOUtils.readInt(buffer);
          break;
        case INT64:
          values[i] = ReadWriteIOUtils.readLong(buffer);
          break;
        case FLOAT:
          values[i] = ReadWriteIOUtils.readFloat(buffer);
          break;
        case DOUBLE:
          values[i] = ReadWriteIOUtils.readDouble(buffer);
          break;
        case TEXT:
          values[i] = ReadWriteIOUtils.readBinary(buffer);
          break;
        default:
          throw new QueryProcessException("Unsupported data type:" + dataTypes[i]);
      }
    }
  }

  public boolean checkDataType(Map<String, MeasurementSchema> schemaMap) {
    return false;
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitInsertRow(this, context);
  }
}
