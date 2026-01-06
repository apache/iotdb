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
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public abstract class AbstractIntoOperator implements ProcessOperator {

  protected final OperatorContext operatorContext;
  protected final Operator child;

  protected TsBlock cachedTsBlock;
  protected DataNodeInternalClient client;

  protected final ExecutorService writeOperationExecutor;
  protected ListenableFuture<TSStatus> writeOperationFuture;

  protected boolean finished = false;

  protected int maxRowNumberInStatement;
  protected long maxReturnSize;

  protected final List<TSDataType> inputColumnTypes;

  protected AbstractIntoOperator(
      OperatorContext operatorContext,
      Operator child,
      List<TSDataType> inputColumnTypes,
      ExecutorService intoOperationExecutor,
      long statementSizePerLine) {
    this.operatorContext = operatorContext;
    this.child = child;
    this.inputColumnTypes = inputColumnTypes;

    this.writeOperationExecutor = intoOperationExecutor;
    setMaxRowNumberInStatement(statementSizePerLine);
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    ListenableFuture<?> childBlocked = child.isBlocked();
    boolean childDone = childBlocked.isDone();
    boolean writeDone = writeOperationDone();
    if (writeDone && childDone) {
      return NOT_BLOCKED;
    } else if (childDone) {
      return writeOperationFuture;
    } else if (writeDone) {
      return childBlocked;
    } else {
      return successfulAsList(Arrays.asList(writeOperationFuture, childBlocked));
    }
  }

  @Override
  public boolean hasNext() throws Exception {
    return !finished;
  }

  @Override
  public TsBlock next() throws Exception {
    checkLastWriteOperation();

    if (!processTsBlock(cachedTsBlock)) {
      return tryToReturnPartialResult();
    }
    cachedTsBlock = null;
    if (child.hasNextWithTimer()) {
      TsBlock inputTsBlock = child.nextWithTimer();
      processTsBlock(inputTsBlock);

      // call child.next only once
      return tryToReturnPartialResult();
    } else {
      return tryToReturnResultTsBlock();
    }
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
    return Math.max(
        child.calculateMaxPeekMemoryWithCounter(),
        calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext();
  }

  @TestOnly
  public long getMaxRowNumberInStatement() {
    return maxRowNumberInStatement;
  }

  /**
   * Check whether the last write operation was executed successfully, and throw an exception if the
   * execution failed, otherwise continue to execute the operator.
   *
   * @throws IntoProcessException wrap InterruptedException with IntoProcessException while
   *     Interruption happened
   */
  protected void checkLastWriteOperation() {
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

      resetInsertTabletStatementGenerators();

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

  protected abstract TsBlock tryToReturnPartialResult();

  protected abstract void resetInsertTabletStatementGenerators();

  private void setMaxRowNumberInStatement(long statementSizePerLine) {
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
  }

  private boolean writeOperationDone() {
    if (writeOperationFuture == null) {
      return true;
    }

    return writeOperationFuture.isDone();
  }
}
