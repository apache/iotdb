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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.storageengine.load;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.common.PipeTransferIntegrityUtils;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.exception.TsFileSequenceReaderTimeseriesMetadataIteratorException;

import java.io.EOFException;
import java.io.File;
import java.nio.BufferUnderflowException;

public final class TsFileStructureCorruptionUtils {

  private TsFileStructureCorruptionUtils() {}

  public static String createDiagnosticMessage(
      final File tsFile, final long offset, final Throwable cause) {
    final String details =
        cause.getMessage() == null ? cause.getClass().getName() : cause.getMessage();
    return String.format(
        StorageEngineMessages
            .EXCEPTION_TSFILE_STRUCTURE_CORRUPTION_TSFILE_ARG_IS_STRUCTURALLY_CORRUPTED_AT_OFFSET_ARG_1_MEANS_UNKNOWN_SHA_256_ARG_ARG_71F6105D,
        tsFile.getAbsolutePath(),
        offset,
        PipeTransferIntegrityUtils.calculateSha256OrUnavailable(
            tsFile, DataNodePipeMessages.MESSAGE_UNKNOWN_AD921D60),
        details);
  }

  public static boolean isStructureCorruption(final Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (current instanceof BufferUnderflowException
          || current instanceof EOFException
          || current instanceof TsFileSequenceReaderTimeseriesMetadataIteratorException) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  public static boolean hasDiagnosticMessage(final Throwable throwable) {
    Throwable current = throwable;
    while (current != null) {
      if (PipeTransferIntegrityUtils.isStructureCorruption(current.getMessage())) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
