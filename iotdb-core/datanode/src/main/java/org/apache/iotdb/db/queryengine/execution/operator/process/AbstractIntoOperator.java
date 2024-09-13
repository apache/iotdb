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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.IntoProcessException;
import org.apache.iotdb.db.protocol.client.DataNodeInternalClient;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public abstract class AbstractIntoOperator implements ProcessOperator {

  protected final OperatorContext operatorContext;
  protected final Operator child;

  protected TsBlock cachedTsBlock;

  protected List<InsertTabletStatementGenerator> insertTabletStatementGenerators;

  private DataNodeInternalClient client;

  private final ExecutorService writeOperationExecutor;
  private ListenableFuture<TSStatus> writeOperationFuture;

  protected boolean finished = false;

  protected int maxRowNumberInStatement;
  private long maxRetainedSize;
  private long maxReturnSize;

  protected final List<Type> typeConvertors;

  private static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  protected AbstractIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.typeConvertors =
        inputColumnTypes.stream().map(TypeFactory::getType).collect(Collectors.toList());

    this.writeOperationExecutor = intoOperationExecutor;
    initMemoryEstimates(statementSizePerLine);
  }

  private void initMemoryEstimates(long statementSizePerLine) {
    long intoOperationBufferSizeInByte =
        IoTDBDescriptor.getInstance().getConfig().getIntoOperationBufferSizeInByte();
    long memAllowedMaxRowNumber = Math.max(intoOperationBufferSizeInByte / statementSizePerLine, 1);
    if (memAllowedMaxRowNumber > Integer.MAX_VALUE) {
      memAllowedMaxRowNumber = Integer.MAX_VALUE;
    }
    this.maxRowNumberInStatement =
        Math.min(
            (int) memAllowedMaxRowNumber,
            IoTDBDescriptor.getInstance().getConfig().getSelectIntoInsertTabletPlanRowLimit());
    long maxStatementSize = maxRowNumberInStatement * statementSizePerLine;

    this.maxRetainedSize = child.calculateMaxReturnSize() + maxStatementSize;
    this.maxReturnSize = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> childBlocked = child.isBlocked();
    boolean writeDone = writeOperationDone();
    if (writeDone && childBlocked.isDone()) {
      return NOT_BLOCKED;
    } else if (childBlocked.isDone()) {
      return writeOperationFuture;
    } else if (writeDone) {
      return childBlocked;
    } else {
      return successfulAsList(Arrays.asList(writeOperationFuture, childBlocked));
    }
  }

  private boolean writeOperationDone() {
    if (writeOperationFuture == null) {
      return true;
    }

    return writeOperationFuture.isDone();
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished;
  }

  @Override
  public TsBlock next() throws Exception {
    checkLastWriteOperation();

    if (!processTsBlock(cachedTsBlock)) {
      return null;
    }
    cachedTsBlock = null;
    if (child.hasNextWithTimer()) {
      TsBlock inputTsBlock = child.nextWithTimer();
      processTsBlock(inputTsBlock);

      // call child.next only once
      return null;
    } else {
      return tryToReturnResultTsBlock();
    }
  }

  /**
   * Check whether the last write operation was executed successfully, and throw an exception if the
   * execution failed, otherwise continue to execute the operator.
   *
   * @throws IntoProcessException wrap InterruptedException with IntoProcessException while
   *     Interruption happened
   */
  private void checkLastWriteOperation() {
    if (writeOperationFuture == null) {
      return;
    }

    try {
      if (!writeOperationFuture.isDone()) {
        throw new IllegalStateException(
            "The operator cannot continue until the last write operation is done.");
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
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IntoProcessException(e.getMessage());
    } catch (ExecutionException e) {
      throw new IntoProcessException(e.getMessage());
    }
  }

  /**
   * Write the data of the input TsBlock into Statement.
   *
   * <p>If the Statement is full, submit one write task and return false.
   *
   * <p>If TsBlock is empty, or all data has been written to Statement, return true.
   */
  protected abstract boolean processTsBlock(TsBlock inputTsBlock);

  protected abstract TsBlock tryToReturnResultTsBlock();

  protected static List<InsertTabletStatementGenerator> constructInsertTabletStatementGenerators(
      Map<PartialPath, Map<String, InputLocation>> targetPathToSourceInputLocationMap,
      Map<PartialPath, Map<String, TSDataType>> targetPathToDataTypeMap,
      Map<String, Boolean> targetDeviceToAlignedMap,
      List<Type> sourceTypeConvertors,
      int maxRowNumberInStatement) {
    List<InsertTabletStatementGenerator> insertTabletStatementGenerators =
        new ArrayList<>(targetPathToSourceInputLocationMap.size());
    for (Map.Entry<PartialPath, Map<String, InputLocation>> entry :
        targetPathToSourceInputLocationMap.entrySet()) {
      PartialPath targetDevice = entry.getKey();
      InsertTabletStatementGenerator generator =
          new InsertTabletStatementGenerator(
              targetDevice,
              entry.getValue(),
              targetPathToDataTypeMap.get(targetDevice),
              targetDeviceToAlignedMap.get(targetDevice.toString()),
              sourceTypeConvertors,
              maxRowNumberInStatement);
      insertTabletStatementGenerators.add(generator);
    }
    return insertTabletStatementGenerators;
  }

  /** Return true if write task is submitted successfully. */
  protected boolean insertMultiTabletsInternally(boolean needCheck) {
    final InsertMultiTabletsStatement insertMultiTabletsStatement =
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

  private boolean existFullStatement(
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (generator.isFull()) {
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
  public boolean isFinished() throws Exception {
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
    return maxReturnSize + maxRetainedSize + child.calculateMaxPeekMemoryWithCounter();
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return maxRetainedSize + child.calculateRetainedSizeAfterCallingNext();
  }

  @TestOnly
  public int getMaxRowNumberInStatement() {
    return maxRowNumberInStatement;
  }

  public static class InsertTabletStatementGenerator {

    private final int rowLimit;

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

    private final List<Type> sourceTypeConvertors;

    public InsertTabletStatementGenerator(
        PartialPath devicePath,
        Map<String, InputLocation> measurementToInputLocationMap,
        Map<String, TSDataType> measurementToDataTypeMap,
        Boolean isAligned,
        List<Type> sourceTypeConvertors,
        int rowLimit) {
      this.devicePath = devicePath;
      this.isAligned = isAligned;
      this.measurements = measurementToInputLocationMap.keySet().toArray(new String[0]);
      this.dataTypes = measurementToDataTypeMap.values().toArray(new TSDataType[0]);
      this.inputLocations = measurementToInputLocationMap.values().toArray(new InputLocation[0]);
      this.writtenCounter = new HashMap<>();
      for (String measurement : measurements) {
        writtenCounter.put(measurement, new AtomicInteger(0));
      }
      this.sourceTypeConvertors = sourceTypeConvertors;
      this.rowLimit = rowLimit;
      this.reset();
    }

    public void reset() {
      this.rowCount = 0;
      this.times = new long[rowLimit];
      this.columns = new Object[this.measurements.length];
      for (int i = 0; i < this.measurements.length; i++) {
        switch (dataTypes[i]) {
          case BOOLEAN:
            columns[i] = new boolean[rowLimit];
            break;
          case INT32:
          case DATE:
            columns[i] = new int[rowLimit];
            break;
          case INT64:
          case TIMESTAMP:
            columns[i] = new long[rowLimit];
            break;
          case FLOAT:
            columns[i] = new float[rowLimit];
            break;
          case DOUBLE:
            columns[i] = new double[rowLimit];
            break;
          case TEXT:
          case STRING:
          case BLOB:
            columns[i] = new Binary[rowLimit];
            Arrays.fill((Binary[]) columns[i], Binary.EMPTY_VALUE);
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format("Data type %s is not supported.", dataTypes[i]));
        }
      }
      this.bitMaps = new BitMap[this.measurements.length];
      for (int i = 0; i < this.bitMaps.length; ++i) {
        this.bitMaps[i] = new BitMap(rowLimit);
        this.bitMaps[i].markAll();
      }
    }

    public int processTsBlock(TsBlock tsBlock, int lastReadIndex) {
      while (lastReadIndex < tsBlock.getPositionCount()) {

        times[rowCount] = tsBlock.getTimeByIndex(lastReadIndex);

        for (int i = 0; i < measurements.length; ++i) {
          int valueColumnIndex = inputLocations[i].getValueColumnIndex();
          Column valueColumn = tsBlock.getValueColumns()[valueColumnIndex];
          Type sourceTypeConvertor = sourceTypeConvertors.get(valueColumnIndex);

          // if the value is NULL
          if (valueColumn.isNull(lastReadIndex)) {
            // bit in bitMaps are marked as 1 (NULL) by default
            continue;
          }

          bitMaps[i].unmark(rowCount);
          writtenCounter.get(measurements[i]).getAndIncrement();
          switch (dataTypes[i]) {
            case INT32:
            case DATE:
              ((int[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getInt(valueColumn, lastReadIndex);
              break;
            case INT64:
            case TIMESTAMP:
              ((long[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getLong(valueColumn, lastReadIndex);
              break;
            case FLOAT:
              ((float[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getFloat(valueColumn, lastReadIndex);
              break;
            case DOUBLE:
              ((double[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getDouble(valueColumn, lastReadIndex);
              break;
            case BOOLEAN:
              ((boolean[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getBoolean(valueColumn, lastReadIndex);
              break;
            case TEXT:
            case BLOB:
            case STRING:
              ((Binary[]) columns[i])[rowCount] =
                  sourceTypeConvertor.getBinary(valueColumn, lastReadIndex);
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
        if (rowCount == rowLimit) {
          break;
        }
      }
      return lastReadIndex;
    }

    public boolean isFull() {
      return rowCount == rowLimit;
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

      if (rowCount != rowLimit) {
        times = Arrays.copyOf(times, rowCount);
        for (int i = 0; i < columns.length; i++) {
          bitMaps[i] = bitMaps[i].getRegion(0, rowCount);
          switch (dataTypes[i]) {
            case BOOLEAN:
              columns[i] = Arrays.copyOf((boolean[]) columns[i], rowCount);
              break;
            case INT32:
            case DATE:
              columns[i] = Arrays.copyOf((int[]) columns[i], rowCount);
              break;
            case INT64:
            case TIMESTAMP:
              columns[i] = Arrays.copyOf((long[]) columns[i], rowCount);
              break;
            case FLOAT:
              columns[i] = Arrays.copyOf((float[]) columns[i], rowCount);
              break;
            case DOUBLE:
              columns[i] = Arrays.copyOf((double[]) columns[i], rowCount);
              break;
            case TEXT:
            case STRING:
            case BLOB:
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
