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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.client.DataNodeInternalClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.IntoProcessException;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.mpp.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public abstract class AbstractIntoOperator implements ProcessOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIntoOperator.class);

  protected final OperatorContext operatorContext;
  protected final Operator child;

  protected TsBlock cachedTsBlock;

  protected List<InsertTabletStatementGenerator> insertTabletStatementGenerators;

  protected final Map<String, InputLocation> sourceColumnToInputLocationMap;

  private DataNodeInternalClient client;

  private final ExecutorService writeOperationExecutor;
  private ListenableFuture<TSStatus> writeOperationFuture;

  protected boolean finished = false;

  private final long maxRetainedSize;
  private final long maxReturnSize;

  public AbstractIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators,
      Map<String, InputLocation> sourceColumnToInputLocationMap,
      ExecutorService intoOperationExecutor,
      long maxStatementSize,
      long maxReturnSize) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.insertTabletStatementGenerators = insertTabletStatementGenerators;
    this.sourceColumnToInputLocationMap = sourceColumnToInputLocationMap;
    this.writeOperationExecutor = intoOperationExecutor;

    this.maxRetainedSize = child.calculateMaxReturnSize() + maxStatementSize;
    this.maxReturnSize = maxReturnSize;
  }

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (PartialPath targetDevice : targetPathToSourceInputLocationMap.keySet()) {
      InsertTabletStatementGenerator generator =
          new InsertTabletStatementGenerator(
              targetDevice,
              targetPathToSourceInputLocationMap.get(targetDevice),
              targetPathToDataTypeMap.get(targetDevice),
              targetDeviceToAlignedMap.get(targetDevice.toString()));
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  /** Return true if write task is submitted successfully. */
  protected boolean insertMultiTabletsInternally(boolean needCheck) {
    InsertMultiTabletsStatement insertMultiTabletsStatement =
        constructInsertMultiTabletsStatement(needCheck);
    if (insertMultiTabletsStatement == null) {
      return false;
    }

    executeInsertMultiTabletsStatement(insertMultiTabletsStatement);
    return true;
  }

  protected InsertMultiTabletsStatement constructInsertMultiTabletsStatement(boolean needCheck) {
    if (insertTabletStatementGenerators == null
        || (needCheck && !existFullStatement(insertTabletStatementGenerators))) {
      return null;
    }

    List<InsertTabletStatement> insertTabletStatementList = new ArrayList<>();
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!generator.isEmpty()) {
        insertTabletStatementList.add(generator.constructInsertTabletStatement());
      }
    }
    if (insertTabletStatementList.isEmpty()) {
      return null;
    }

    InsertMultiTabletsStatement insertMultiTabletsStatement = new InsertMultiTabletsStatement();
    insertMultiTabletsStatement.setInsertTabletStatementList(insertTabletStatementList);
    return insertMultiTabletsStatement;
  }

  protected void executeInsertMultiTabletsStatement(
      InsertMultiTabletsStatement insertMultiTabletsStatement) {
    if (client == null) {
      client = new DataNodeInternalClient(operatorContext.getSessionInfo());
    }

    writeOperationFuture =
        Futures.submit(
            () -> client.insertTablets(insertMultiTabletsStatement), writeOperationExecutor);
  }

  /** Return true if the previous write task has done. */
  protected boolean processWriteOperationFuture() {
    if (writeOperationFuture == null) {
      return true;
    }

    try {
      if (!writeOperationFuture.isDone()) {
        return false;
      }

      TSStatus executionStatus = writeOperationFuture.get();
      if (executionStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()
          && executionStatus.getCode() != TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
        String message =
            String.format(
                "Error occurred while inserting tablets in SELECT INTO: %s",
                executionStatus.getMessage());
        throw new IntoProcessException(message);
      }

      for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
        generator.reset();
      }

      writeOperationFuture = null;
      return true;
    } catch (InterruptedException e) {
      LOGGER.warn(
          "{}: interrupted when processing write operation future with exception {}", this, e);
      Thread.currentThread().interrupt();
      throw new IntoProcessException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IntoProcessException(e.getMessage());
    }
  }

  private boolean existFullStatement(
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (generator.isFull()) {
        return true;
      }
    }
    return false;
  }

  private boolean existNonEmptyStatement(
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators) {
    if (insertTabletStatementGenerators == null) {
      return false;
    }
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (generator != null && !generator.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  protected int findWritten(String device, String measurement) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (!Objects.equals(generator.getDevice(), device)) {
        continue;
      }
      return generator.getWrittenCount(measurement);
    }
    return 0;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  private boolean writeOperationDone() {
    if (writeOperationFuture == null) {
      return true;
    }

    return writeOperationFuture.isDone();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> childBlocked = child.isBlocked();
    if (writeOperationDone() && childBlocked.isDone()) {
      return NOT_BLOCKED;
    } else if (!writeOperationDone() && childBlocked.isDone()) {
      return writeOperationFuture;
    } else if (writeOperationDone() && !childBlocked.isDone()) {
      return childBlocked;
    } else {
      return successfulAsList(Arrays.asList(writeOperationFuture, childBlocked));
    }
  }

  @Override
  public boolean hasNext() {
    return !finished;
  }

  @Override
  public boolean isFinished() {
    return finished;
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
    if (writeOperationFuture != null) {
      writeOperationFuture.cancel(true);
    }
    child.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    return maxReturnSize + maxRetainedSize + child.calculateMaxPeekMemory();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }

  public static class InsertTabletStatementGenerator {

    private final int TABLET_ROW_LIMIT =
        IoTDBDescriptor.getInstance().getConfig().getSelectIntoInsertTabletPlanRowLimit();

    private final PartialPath devicePath;
    private final boolean isAligned;
    private final String[] measurements;
    private final TSDataType[] dataTypes;
    private final InputLocation[] inputLocations;

    private int rowCount = 0;

    private long[] times;
    private Object[] columns;
    private BitMap[] bitMaps;

    private final Map<String, AtomicInteger> writtenCounter;

    public InsertTabletStatementGenerator(
        PartialPath devicePath,
        Map<String, InputLocation> measurementToInputLocationMap,
        Map<String, TSDataType> measurementToDataTypeMap,
        Boolean isAligned) {
      this.devicePath = devicePath;
      this.isAligned = isAligned;
      this.measurements = measurementToInputLocationMap.keySet().toArray(new String[0]);
      this.dataTypes = measurementToDataTypeMap.values().toArray(new TSDataType[0]);
      this.inputLocations = measurementToInputLocationMap.values().toArray(new InputLocation[0]);
      this.writtenCounter = new HashMap<>();
      for (String measurement : measurements) {
        writtenCounter.put(measurement, new AtomicInteger(0));
      }
      this.reset();
    }

    public void reset() {
      this.rowCount = 0;
      this.times = new long[TABLET_ROW_LIMIT];
      this.columns = new Object[this.measurements.length];
      for (int i = 0; i < this.measurements.length; i++) {
        switch (dataTypes[i]) {
          case BOOLEAN:
            columns[i] = new boolean[TABLET_ROW_LIMIT];
            break;
          case INT32:
            columns[i] = new int[TABLET_ROW_LIMIT];
            break;
          case INT64:
            columns[i] = new long[TABLET_ROW_LIMIT];
            break;
          case FLOAT:
            columns[i] = new float[TABLET_ROW_LIMIT];
            break;
          case DOUBLE:
            columns[i] = new double[TABLET_ROW_LIMIT];
            break;
          case TEXT:
            columns[i] = new Binary[TABLET_ROW_LIMIT];
            Arrays.fill((Binary[]) columns[i], Binary.EMPTY_VALUE);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes[i]));
        }
      }
      this.bitMaps = new BitMap[this.measurements.length];
      for (int i = 0; i < this.bitMaps.length; ++i) {
        this.bitMaps[i] = new BitMap(TABLET_ROW_LIMIT);
        this.bitMaps[i].markAll();
      }
    }

    public int processTsBlock(TsBlock tsBlock, int lastReadIndex) {
      while (lastReadIndex < tsBlock.getPositionCount()) {

        times[rowCount] = tsBlock.getTimeByIndex(lastReadIndex);

        for (int i = 0; i < measurements.length; ++i) {
          Column valueColumn = tsBlock.getValueColumns()[inputLocations[i].getValueColumnIndex()];

          // if the value is NULL
          if (valueColumn.isNull(lastReadIndex)) {
            // bit in bitMaps are marked as 1 (NULL) by default
            continue;
          }

          bitMaps[i].unmark(rowCount);
          writtenCounter.get(measurements[i]).getAndIncrement();
          switch (valueColumn.getDataType()) {
            case INT32:
              ((int[]) columns[i])[rowCount] = valueColumn.getInt(lastReadIndex);
              break;
            case INT64:
              ((long[]) columns[i])[rowCount] = valueColumn.getLong(lastReadIndex);
              break;
            case FLOAT:
              ((float[]) columns[i])[rowCount] = valueColumn.getFloat(lastReadIndex);
              break;
            case DOUBLE:
              ((double[]) columns[i])[rowCount] = valueColumn.getDouble(lastReadIndex);
              break;
            case BOOLEAN:
              ((boolean[]) columns[i])[rowCount] = valueColumn.getBoolean(lastReadIndex);
              break;
            case TEXT:
              ((Binary[]) columns[i])[rowCount] = valueColumn.getBinary(lastReadIndex);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format(
                      "data type %s is not supported when convert data at client",
                      valueColumn.getDataType()));
          }
        }

        ++rowCount;
        ++lastReadIndex;
        if (rowCount == TABLET_ROW_LIMIT) {
          break;
        }
      }
      return lastReadIndex;
    }

    public boolean isFull() {
      return rowCount == TABLET_ROW_LIMIT;
    }

    public boolean isEmpty() {
      return rowCount == 0;
    }

    public InsertTabletStatement constructInsertTabletStatement() {
      InsertTabletStatement insertTabletStatement = new InsertTabletStatement();
      insertTabletStatement.setDevicePath(devicePath);
      insertTabletStatement.setAligned(isAligned);
      insertTabletStatement.setMeasurements(measurements);
      insertTabletStatement.setDataTypes(dataTypes);
      insertTabletStatement.setRowCount(rowCount);

      if (rowCount != TABLET_ROW_LIMIT) {
        times = Arrays.copyOf(times, rowCount);
        for (int i = 0; i < columns.length; i++) {
          bitMaps[i] = bitMaps[i].getRegion(0, rowCount);
          switch (dataTypes[i]) {
            case BOOLEAN:
              columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
              break;
            case INT32:
              columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
              break;
            case INT64:
              columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
              break;
            case FLOAT:
              columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
              break;
            case DOUBLE:
              columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
              break;
            case TEXT:
              columns[i] = Arrays.copyOf((Binary[]) columns[i], rowCount);
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", dataTypes[i]));
          }
        }
      }

      insertTabletStatement.setTimes(times);
      insertTabletStatement.setBitMaps(bitMaps);
      insertTabletStatement.setColumns(columns);

      return insertTabletStatement;
    }

    public String getDevice() {
      return devicePath.toString();
    }

    public int getWrittenCount(String measurement) {
      if (!writtenCounter.containsKey(measurement)) {
        return -1;
      }
      return writtenCounter.get(measurement).get();
    }
  }
}
