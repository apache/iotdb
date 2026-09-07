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

package org.apache.iotdb.db.exception;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.File;

/** Thrown when a TsFile is detected to be corrupted during query execution. */
public class CorruptedTsFileException extends IoTDBRuntimeException {

  public enum Stage {
    READ_TIMESERIES_METADATA,
    READ_CHUNK_DATA_OR_LOAD_PAGE_READER,
    DECODE_PAGE_DATA,
    READ_METADATA_INDEX_NODE
  }

  private final File tsFile;
  private final Stage stage;

  /**
   * Creates a CorruptedTsFileException.
   *
   * <p>The original exception {@code cause} is added via {@link #addSuppressed(Throwable)} rather
   * than {@link #initCause(Throwable)}. This ensures that {@code
   * ErrorHandlingCommonUtils.getRootCause(this)} returns this exception itself (not the wrapped
   * IOException), so upstream error handling in {@code AbstractDriverThread} matches it correctly.
   * The original cause stack trace is preserved via suppressed exceptions.
   *
   * @param tsFile the corrupted TsFile
   * @param stage the operation that encountered the corruption
   * @param message user-facing error message
   * @param cause the original exception (preserved as suppressed)
   */
  public CorruptedTsFileException(File tsFile, Stage stage, String message, Throwable cause) {
    super(message, TSStatusCode.CANNOT_READ_TSFILE.getStatusCode());
    this.tsFile = tsFile;
    this.stage = stage;
    if (cause != null) {
      addSuppressed(cause);
    }
  }

  public File getTsFile() {
    return tsFile;
  }

  public Stage getStage() {
    return stage;
  }
}
