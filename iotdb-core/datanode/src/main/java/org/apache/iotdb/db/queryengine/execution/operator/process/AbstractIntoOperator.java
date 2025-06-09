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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.IntoProcessException;
import org.apache.iotdb.db.protocol.client.DataNodeInternalClient;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertMultiTabletsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertTabletStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
        throw new IntoProcessException(message, executionStatus.getCode());
      }

      for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
        generator.reset();
      }

      writeOperationFuture = null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IntoProcessException(e);
    } catch (ExecutionException e) {
      throw new IntoProcessException(e);
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
            () ->
                insertMultiTabletsStatement.isWriteToTable()
                    ? client.insertRelationalTablets(insertMultiTabletsStatement)
                    : client.insertTablets(insertMultiTabletsStatement),
            writeOperationExecutor);
  }

  protected boolean existFullStatement(
      List<InsertTabletStatementGenerator> insertTabletStatementGenerators) {
    for (InsertTabletStatementGenerator generator : insertTabletStatementGenerators) {
      if (generator.isFull()) {
        return true;
      }
    }
    return false;
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
}
