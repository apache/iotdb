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

import org.apache.iotdb.cli.i18n.CliMessages;
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
            String.format(
                CliMessages
                    .LOG_REJECTING_FILE_ARG_DUE_TO_MEMORY_CONSTRAINTS_WILL_RETRY_LATER_AE83A15F,
                Objects.toString(filePath)));
        ImportTsFileScanTool.putToQueue(filePath);
        return;
      }

      loadFileFailedNum.increment();
      IOT_PRINTER.println(
          String.format(
              CliMessages.LOG_FAILED_TO_IMPORT_ARG_FILE_ARG_F085D907,
              Objects.toString(filePath),
              Objects.toString(e.getMessage())));

      try {
        processingFile(filePath, false);
        processingLoadFailedFileSuccessfulNum.increment();
        IOT_PRINTER.println(
            String.format(
                CliMessages.LOG_PROCESSED_FAIL_FILE_ARG_SUCCESSFULLY_B430F7F0,
                Objects.toString(filePath)));
      } catch (final Exception processFailException) {
        IOT_PRINTER.println(
            String.format(
                CliMessages.LOG_FAILED_TO_PROCESS_FAIL_FILE_ARG_ARG_D01A0F75,
                Objects.toString(filePath),
                Objects.toString(processFailException.getMessage())));
      }
    } catch (final InterruptedException e1) {
      IOT_PRINTER.println(
          String.format(
              CliMessages.EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331,
              Objects.toString(e1.getMessage())));
      Thread.currentThread().interrupt();
    } catch (final Exception e1) {
      IOT_PRINTER.println(
          String.format(
              CliMessages.EXCEPTION_UNEXPECTED_ERROR_OCCURRED_ARG_402FA331,
              Objects.toString(e1.getMessage())));
    }
  }

  protected static void processSuccessFile(final String filePath) {
    loadFileSuccessfulNum.increment();
    IOT_PRINTER.println(
        String.format(
            CliMessages.LOG_IMPORTED_ARG_FILE_SUCCESSFULLY_B323C7DC, Objects.toString(filePath)));

    try {
      processingFile(filePath, true);
      processingLoadSuccessfulFileSuccessfulNum.increment();
      IOT_PRINTER.println(
          String.format(
              CliMessages.LOG_PROCESSED_SUCCESS_FILE_ARG_SUCCESSFULLY_B2CCF2E5,
              Objects.toString(filePath)));
    } catch (final Exception processSuccessException) {
      IOT_PRINTER.println(
          String.format(
              CliMessages.LOG_FAILED_TO_PROCESS_SUCCESS_FILE_ARG_ARG_8E0E4B9C,
              Objects.toString(filePath),
              Objects.toString(processSuccessException.getMessage())));
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
        String.format(
            CliMessages
                .LOG_SUCCESSFULLY_LOAD_ARG_TSFILE_S_ON_SUCCESS_OPERATION_S_ARG_SUCCEED_ARG_FAILED_A04B748F,
            Objects.toString(loadFileSuccessfulNum.sum()),
            Objects.toString(processingLoadSuccessfulFileSuccessfulNum.sum()),
            Objects.toString(
                (loadFileSuccessfulNum.sum() - processingLoadSuccessfulFileSuccessfulNum.sum()))));
    IOT_PRINTER.println(
        String.format(
            CliMessages
                .LOG_FAILED_TO_LOAD_ARG_FILE_S_ON_FAIL_OPERATION_S_ARG_SUCCEED_ARG_FAILED_F5B202E2,
            Objects.toString(loadFileFailedNum.sum()),
            Objects.toString(processingLoadFailedFileSuccessfulNum.sum()),
            Objects.toString(
                (loadFileFailedNum.sum() - processingLoadFailedFileSuccessfulNum.sum()))));
    IOT_PRINTER.println(
        String.format(
            CliMessages.LOG_UNPROCESSED_ARG_FILE_S_DUE_TO_UNEXPECTED_EXCEPTIONS_0950B97D,
            Objects.toString(ImportTsFileScanTool.getTsFileQueueSize())));
    IOT_PRINTER.println(CliMessages.LOG_FOR_MORE_DETAILS_PLEASE_CHECK_THE_LOG_D0597775);
    IOT_PRINTER.println(
        String.format(
            CliMessages.LOG_TOTAL_OPERATION_TIME_ARG_MS_D8632871,
            Objects.toString((System.currentTimeMillis() - startTime))));
    IOT_PRINTER.println(CliMessages.LOG_WORK_HAS_BEEN_COMPLETED_B4F48618);
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
