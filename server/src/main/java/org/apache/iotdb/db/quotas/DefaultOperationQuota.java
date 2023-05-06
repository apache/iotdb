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

package org.apache.iotdb.db.quotas;

import org.apache.iotdb.commons.exception.RpcThrottlingException;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsOfOneDeviceStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertRowsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.utils.TypeInferenceUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;
import java.util.List;

public class DefaultOperationQuota implements OperationQuota {
  protected final QuotaLimiter limiter;
  // the available read/write quota size in bytes
  protected long readAvailable = 0;
  // estimated quota
  protected long writeConsumed = 0;
  protected long readConsumed = 0;

  // real consumed quota
  private long[] operationSize;
  // difference between estimated quota and real consumed quota used in close method
  // to adjust quota amount. Also used by ExceedOperationQuota which is a subclass
  // of DefaultOperationQuota
  protected long writeDiff = 0;
  protected long readDiff = 0;

  public DefaultOperationQuota(final QuotaLimiter limiter) {
    this.limiter = limiter;
    int size = OperationType.values().length;
    operationSize = new long[size];
    for (int i = 0; i < size; i++) {
      operationSize[i] = 0;
    }
  }

  @Override
  public void checkQuota(int numWrites, int numReads, Statement s) throws RpcThrottlingException {
    updateEstimateConsumeQuota(numWrites, numReads, s);

    readAvailable = Long.MAX_VALUE;
    limiter.checkQuota(numWrites, writeConsumed, numReads, readConsumed);
    readAvailable = Math.min(readAvailable, limiter.getReadAvailable());

    limiter.grabQuota(numWrites, writeConsumed, numReads, readConsumed);
  }

  @Override
  public void addReadResult(List<ByteBuffer> queryResult) {
    if (queryResult == null) {
      return;
    }
    long size = 0;
    for (ByteBuffer buffer : queryResult) {
      size += buffer.limit();
    }
    operationSize[OperationType.READ.ordinal()] += size;
  }

  /**
   * Update estimate quota(read/write size/capacityUnits) which will be consumed
   *
   * @param numWrites the number of write requests
   * @param numReads the number of read requests
   */
  protected void updateEstimateConsumeQuota(int numWrites, int numReads, Statement s) {
    if (numWrites > 0) {
      long avgSize = 0;
      switch (s.getType()) {
        case INSERT:
          // InsertStatement  InsertRowStatement
          if (s instanceof InsertStatement) {
            InsertStatement insertStatement = (InsertStatement) s;
            for (int i = 0; i < insertStatement.getValuesList().size(); i++) {
              avgSize += calculationWrite(insertStatement.getValuesList().get(i));
            }
          }
          if (s instanceof InsertRowStatement) {
            InsertRowStatement insertRowStatement = (InsertRowStatement) s;
            avgSize += calculationWrite(insertRowStatement.getValues());
          }
          break;
        case BATCH_INSERT:
          // InsertTabletStatement
          InsertTabletStatement insertTabletStatement = (InsertTabletStatement) s;
          avgSize += calculationWrite(insertTabletStatement.getColumns());
          break;
        case BATCH_INSERT_ONE_DEVICE:
          // InsertRowsOfOneDeviceStatement
          InsertRowsOfOneDeviceStatement insertRowsOfOneDeviceStatement =
              (InsertRowsOfOneDeviceStatement) s;
          for (InsertRowStatement insertRowStatement :
              insertRowsOfOneDeviceStatement.getInsertRowStatementList()) {
            avgSize += calculationWrite(insertRowStatement.getValues());
          }
          break;
        case BATCH_INSERT_ROWS:
          // InsertRowsStatement
          InsertRowsStatement insertRowsStatement = (InsertRowsStatement) s;
          for (InsertRowStatement insertRowStatement :
              insertRowsStatement.getInsertRowStatementList()) {
            avgSize += calculationWrite(insertRowStatement.getValues());
          }
          break;
        case MULTI_BATCH_INSERT:
          // LoadTsFileStatement  InsertMultiTabletsStatement
          if (s instanceof LoadTsFileStatement) {
            LoadTsFileStatement loadTsFileStatement = (LoadTsFileStatement) s;
            for (int i = 0; i < loadTsFileStatement.getResources().size(); i++) {
              avgSize += loadTsFileStatement.getResources().get(i).getTsFileSize();
            }
          }
          if (s instanceof InsertMultiTabletsStatement) {
            InsertMultiTabletsStatement insertMultiTabletsStatement =
                (InsertMultiTabletsStatement) s;
            for (int i = 0;
                i < insertMultiTabletsStatement.getInsertTabletStatementList().size();
                i++) {
              for (InsertTabletStatement insertTablet :
                  insertMultiTabletsStatement.getInsertTabletStatementList()) {
                avgSize += calculationWrite(insertTablet.getColumns());
              }
            }
          }
          break;
        default:
          throw new RuntimeException("Invalid statement type: " + s.getType());
      }
      writeConsumed = estimateConsume(numWrites, avgSize);
    }
    if (numReads > 0) {
      readConsumed = estimateConsume(numReads, 1000);
    }
  }

  private long calculationWrite(Object[] values) {
    long size = 0;
    for (int i = 0; i < values.length; i++) {
      TSDataType dataType = TypeInferenceUtils.getPredictedDataType(values[i], true);
      assert dataType != null;
      size += dataType.getDataTypeSize();
    }
    return size;
  }

  private long estimateConsume(int numReqs, long avgSize) {
    if (numReqs > 0) {
      return avgSize * numReqs;
    }
    return 0;
  }

  @Override
  public void close() {
    // Adjust the quota consumed for the specified operation
    readDiff = operationSize[OperationType.READ.ordinal()] - readConsumed;
    if (readDiff != 0) {
      limiter.consumeRead(readDiff);
    }
  }
}
