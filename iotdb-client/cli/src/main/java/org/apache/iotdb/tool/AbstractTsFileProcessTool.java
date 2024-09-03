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

package org.apache.iotdb.tool;

import org.apache.iotdb.cli.utils.IoTPrinter;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public abstract class AbstractTsFileProcessTool {

  private static final IoTPrinter ioTPrinter = new IoTPrinter(System.out);

  private static final LongAdder loadFileSuccessfulNum = new LongAdder();
  private static final LongAdder loadFileFailedNum = new LongAdder();
  private static final LongAdder processingLoadSuccessfulFileSuccessfulNum = new LongAdder();
  private static final LongAdder processingLoadFailedFileSuccessfulNum = new LongAdder();

  protected abstract void loadTsFile();

  protected void processFailFile(final String filePath, final Exception e) {
    // Reject because of memory controls, do retry later
    if (Objects.nonNull(e.getMessage()) && e.getMessage().contains("memory")) {
      ioTPrinter.println(
          "Rejecting file [ " + filePath + " ] due to memory constraints, will retry later.");
      IoTDBTsFileScanAndProcessTool.put(filePath);
      return;
    }

    loadFileFailedNum.increment();
    ioTPrinter.println("Failed to import [ " + filePath + " ] file: " + e.getMessage());

    try {
      IoTDBTsFileScanAndProcessTool.processingFile(filePath, false);
      processingLoadFailedFileSuccessfulNum.increment();
      ioTPrinter.println("Processed fail file [ " + filePath + " ] successfully!");
    } catch (Exception processFailException) {
      ioTPrinter.println(
          "Failed to process fail file [ " + filePath + " ]: " + processFailException.getMessage());
    }
  }

  protected static void processSuccessFile(final String filePath) {
    loadFileSuccessfulNum.increment();
    ioTPrinter.println("Imported [ " + filePath + " ] file successfully!");

    try {
      IoTDBTsFileScanAndProcessTool.processingFile(filePath, true);
      processingLoadSuccessfulFileSuccessfulNum.increment();
      ioTPrinter.println("Processed success file [ " + filePath + " ] successfully!");
    } catch (Exception processSuccessException) {
      ioTPrinter.println(
          "Failed to process success file [ "
              + filePath
              + " ]: "
              + processSuccessException.getMessage());
    }
  }

  protected static void printResult(final long startTime) {
    ioTPrinter.println(
        "Successfully load "
            + loadFileSuccessfulNum.sum()
            + " tsfile(s) (--on_success operation(s): "
            + processingLoadSuccessfulFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileSuccessfulNum.sum() - processingLoadSuccessfulFileSuccessfulNum.sum())
            + " failed)");
    ioTPrinter.println(
        "Failed to load "
            + loadFileFailedNum.sum()
            + " file(s) (--on_fail operation(s): "
            + processingLoadFailedFileSuccessfulNum.sum()
            + " succeed, "
            + (loadFileFailedNum.sum() - processingLoadFailedFileSuccessfulNum.sum())
            + " failed)");
    ioTPrinter.println("For more details, please check the log.");
    ioTPrinter.println(
        "Total operation time: " + (System.currentTimeMillis() - startTime) + " ms.");
    ioTPrinter.println("Work has been completed!");
  }
}
