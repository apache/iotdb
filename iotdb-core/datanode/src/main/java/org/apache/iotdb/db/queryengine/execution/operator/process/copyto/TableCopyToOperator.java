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

package org.apache.iotdb.db.queryengine.execution.operator.process.copyto;

import org.apache.iotdb.calc.execution.operator.Operator;
import org.apache.iotdb.calc.execution.operator.process.ProcessOperator;
import org.apache.iotdb.commons.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.CopyToTsFileOptions;
import org.apache.iotdb.db.queryengine.execution.operator.process.copyto.tsfile.TsFileFormatCopyToWriter;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public class TableCopyToOperator implements ProcessOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableCopyToOperator.class);

  private final OperatorContext operatorContext;
  private final Operator childOperator;
  private final String targetFilePath;
  private final CopyToOptions options;
  private final List<ColumnHeader> innerQueryColumnHeaders;
  private final int[] columnIndex2TsBlockColumnIndex;

  private IFormatCopyToWriter writer;
  private File targetFile;
  private boolean isFinished = false;
  private boolean hasData = false;

  public TableCopyToOperator(
      OperatorContext operatorContext,
      Operator child,
      String targetFilePath,
      CopyToOptions options,
      List<ColumnHeader> innerQueryColumnHeaders,
      int[] columnIndex2TsBlockColumnIndex) {
    this.operatorContext = operatorContext;
    this.childOperator = child;
    this.targetFilePath = targetFilePath;
    this.options = options;
    this.innerQueryColumnHeaders = innerQueryColumnHeaders;
    this.columnIndex2TsBlockColumnIndex = columnIndex2TsBlockColumnIndex;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    IFormatCopyToWriter formatWriter = getWriter();
    if (!childOperator.hasNext()) {
      formatWriter.seal();
      isFinished = true;
      return formatWriter.buildResultTsBlock();
    }
    TsBlock tsBlock = childOperator.next();
    if (tsBlock == null || tsBlock.isEmpty()) {
      return null;
    }
    hasData = true;
    formatWriter.write(tsBlock);
    return null;
  }

  private IFormatCopyToWriter getWriter() throws Exception {
    if (writer != null) {
      return writer;
    }
    this.targetFile = createTargetFile(targetFilePath);
    switch (options.getFormat()) {
      case TSFILE:
      default:
        this.writer =
            new TsFileFormatCopyToWriter(
                this.targetFile,
                (CopyToTsFileOptions) options,
                innerQueryColumnHeaders,
                columnIndex2TsBlockColumnIndex);
    }
    return writer;
  }

  private File createTargetFile(String path) throws Exception {
    File file = new File(path);
    if (file.getParent() == null) {
      String dir = TierManager.getInstance().getNextFolderForCopyToTargetFile();
      file = new File(dir, path);
    }
    File parent = file.getParentFile();
    if (parent != null && !parent.exists() && !parent.mkdirs()) {
      throw new IOException("Failed to create directories: " + parent);
    }
    if (file.exists()) {
      throw new IOException("Target file already exists: " + file.getAbsolutePath());
    }
    if (!file.createNewFile()) {
      throw new IOException("Failed to create file: " + file.getAbsolutePath());
    }

    return file;
  }

  @Override
  public boolean hasNext() throws Exception {
    return !isFinished;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return childOperator.isBlocked();
  }

  @Override
  public void close() throws Exception {
    childOperator.close();
    if (writer != null) {
      writer.close();
      writer = null;
    }
    if (targetFile == null || (hasData && isFinished)) {
      return;
    }
    Files.deleteIfExists(targetFile.toPath());
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(
            childOperator.calculateMaxPeekMemory(),
            calculateMaxReturnSize() + calculateRetainedSizeAfterCallingNext())
        + options.estimatedMaxRamBytesInWrite();
  }

  @Override
  public long calculateMaxReturnSize() {
    return TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return childOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + RamUsageEstimator.sizeOf(targetFilePath)
        + RamUsageEstimator.sizeOfObject(innerQueryColumnHeaders)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(columnIndex2TsBlockColumnIndex)
        + childOperator.ramBytesUsed();
  }
}
