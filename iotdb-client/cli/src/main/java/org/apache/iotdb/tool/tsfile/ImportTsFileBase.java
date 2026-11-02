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

package org.apache.iotdb.tool.tsfile;

import org.apache.iotdb.cli.utils.IoTPrinter;
import org.apache.iotdb.tool.common.ImportTsFileOperation;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public abstract class ImportTsFileBase implements Runnable {

  private static final IoTPrinter IOT_PRINTER = new IoTPrinter(System.out);

  private static final LongAdder loadFileSuccessfulNum = new LongAdder();
  private static final LongAdder loadFileFailedNum = new LongAdder();
  private static final LongAdder processingLoadSuccessfulFileSuccessfulNum = new LongAdder();
  private static final LongAdder processingLoadFailedFileSuccessfulNum = new LongAdder();
  private static String timePrecision = "ms";
  private static String successDir;
  private static ImportTsFileOperation successOperation;
  private static String failDir;
  private static ImportTsFileOperation failOperation;

  @Override
  public void run() {
    loadTsFile();
  }

  protected abstract void loadTsFile();

  protected void processFailFile(final String filePath, final Exception e) {
    // Reject because of memory controls, do retry later
    try {
      if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
        IOT_PRINTER.println(
            "Rejecting file [ " + filePath + " ] due to memory constraints, will retry later.");
        ImportTsFileScanTool.putToQueue(filePath);
        return;
      }

      loadFileFailedNum.increment();
      IOT_PRINTER.println("Failed to import [ " + filePath + " ] file: " + e.getMessage());

      try {
        processingFile(filePath, false);
        processingLoadFailedFileSuccessfulNum.increment();
        IOT_PRINTER.println("Processed fail file [ " + filePath + " ] successfully!");
      } catch (final Exception processFailException) {
        IOT_PRINTER.println(
            "Failed to process fail file [ "
                + filePath
                + " ]: "
                + processFailException.getMessage());
      }
    } catch (final InterruptedException e1) {
      IOT_PRINTER.println("Unexpected error occurred: " + e1.getMessage());
      Thread.currentThread().interrupt();
    } catch (final Exception e1) {
      IOT_PRINTER.println("Unexpected error occurred: " + e1.getMessage());
    }
  }

  protected static void processSuccessFile(final String filePath) {
    loadFileSuccessfulNum.increment();
    IOT_PRINTER.println("Imported [ " + filePath + " ] file successfully!");

    try {
      processingFile(filePath, true);
      processingLoadSuccessfulFileSuccessfulNum.increment();
      IOT_PRINTER.println("Processed success file [ " + filePath + " ] successfully!");
    } catch (final Exception processSuccessException) {
      IOT_PRINTER.println(
          "Failed to process success file [ "
              + filePath
              + " ]: "
              + processSuccessException.getMessage());
    }
  }

  public static void processingFile(final String filePath, final boolean isSuccess)
      throws IOException {
    final Path target =
        Paths.get(isSuccess ? successDir : failDir)
            .resolve(ImportTsFileScanTool.getRelativePath(filePath));
    final ImportTsFileOperation operation = isSuccess ? successOperation : failOperation;
    operation.processFileGroup(Paths.get(filePath), target);
  }

  protected static void resetCounters() {
    loadFileSuccessfulNum.reset();
    loadFileFailedNum.reset();
    processingLoadSuccessfulFileSuccessfulNum.reset();
    processingLoadFailedFileSuccessfulNum.reset();
  }

  protected static boolean hasFailures() {
    return loadFileFailedNum.sum() > 0
        || loadFileSuccessfulNum.sum() != processingLoadSuccessfulFileSuccessfulNum.sum()
        || ImportTsFileScanTool.getTsFileQueueSize() > 0;
  }

  protected static void printResult(final long startTime) {
    IOT_PRINTER.println(
        "Successfully load "
            + loadFileSuccessfulNum.sum()
            + " tsfile(s) (--on_success operation(s): "
            + processingLoadSuccessfulFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileSuccessfulNum.sum() - processingLoadSuccessfulFileSuccessfulNum.sum())
            + " failed)");
    IOT_PRINTER.println(
        "Failed to load "
            + loadFileFailedNum.sum()
            + " file(s) (--on_fail operation(s): "
            + processingLoadFailedFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileFailedNum.sum() - processingLoadFailedFileSuccessfulNum.sum())
            + " failed)");
    IOT_PRINTER.println(
        "Unprocessed "
            + ImportTsFileScanTool.getTsFileQueueSize()
            + " file(s), due to unexpected exceptions");
    IOT_PRINTER.println("For more details, please check the log.");
    IOT_PRINTER.println(
        "Total operation time: " + (System.currentTimeMillis() - startTime) + " ms.");
    IOT_PRINTER.println("Work has been completed!");
  }

  public static void setSuccessAndFailDirAndOperation(
      final String successDir,
      final ImportTsFileOperation successOperation,
      final String failDir,
      final ImportTsFileOperation failOperation) {
    ImportTsFileBase.successDir = successDir;
    ImportTsFileBase.successOperation = successOperation;
    ImportTsFileBase.failDir = failDir;
    ImportTsFileBase.failOperation = failOperation;
  }

  protected void setTimePrecision(String timePrecision) {
    this.timePrecision = timePrecision;
  }

  protected String getTimePrecision() {
    return timePrecision;
  }
}
