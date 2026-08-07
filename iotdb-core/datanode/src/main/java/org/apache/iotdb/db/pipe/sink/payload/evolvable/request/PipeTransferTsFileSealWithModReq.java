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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV2;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class PipeTransferTsFileSealWithModReq extends PipeTransferFileSealReqV2 {

  private PipeTransferTsFileSealWithModReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_TS_FILE_SEAL_WITH_MOD;
  }

  protected static final String DATABASE_NAME_KEY_PREFIX = "DATABASE_NAME_";
  private static final String WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY = "WAIT_FOR_SCHEMA_BEFORE_LOAD";
  public static final String CONVERSION_TASK_ID_KEY = "CONVERSION_TASK_ID";
  public static final String ASYNC_LOAD_ON_TYPE_MISMATCH_KEY = "ASYNC_LOAD_ON_TYPE_MISMATCH";
  private static final String UNSUPPORTED_REBOOT_TIMES = "unsupported-reboot-times";
  private static final String UNSUPPORTED_REPLICATE_INDEX = "unsupported-replicate-index";
  private static final String UNSUPPORTED_PROGRESS_INDEX = "unsupported-progress-index";

  public String getDatabaseNameByTsFileName() {
    return parameters == null
        ? null
        : parameters.get(generateDatabaseNameWithFileNameKey(fileNames.get(fileNames.size() - 1)));
  }

  public boolean shouldWaitForSchemaBeforeLoad() {
    return parameters != null
        && Boolean.parseBoolean(parameters.get(WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY));
  }

  public String getConversionTaskId() {
    return parameters == null ? null : parameters.get(CONVERSION_TASK_ID_KEY);
  }

  public boolean shouldAsyncLoadOnTypeMismatch() {
    if (parameters == null) {
      return true;
    }
    final String value = parameters.get(ASYNC_LOAD_ON_TYPE_MISMATCH_KEY);
    return value == null || Boolean.parseBoolean(value);
  }

  public PipeTransferTsFileSealWithModReq setConversionTaskInfo(
      final String conversionTaskId, final boolean shouldAsyncLoadOnTypeMismatch)
      throws IOException {
    if (parameters == null) {
      parameters = new HashMap<>();
    }
    if (conversionTaskId != null) {
      parameters.put(CONVERSION_TASK_ID_KEY, conversionTaskId);
    }
    parameters.put(
        ASYNC_LOAD_ON_TYPE_MISMATCH_KEY, Boolean.toString(shouldAsyncLoadOnTypeMismatch));
    return (PipeTransferTsFileSealWithModReq)
        convertToTPipeTransferReq(fileNames, fileLengths, parameters);
  }

  public static String generateConversionTaskId(
      final String sinkTaskId,
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength) {
    return generateConversionTaskId(
        sinkTaskId, modFileName, modFileLength, tsFileName, tsFileLength, null);
  }

  public static String generateConversionTaskId(
      final String sinkTaskId,
      final Iterable<? extends EnrichedEvent> events,
      final String databaseName,
      final int outputIndex) {
    return generateConversionTaskId(sinkTaskId, events, databaseName, outputIndex, false);
  }

  public static String generateConversionTaskId(
      final String sinkTaskId,
      final Iterable<? extends EnrichedEvent> events,
      final String databaseName,
      final int outputIndex,
      final boolean hasModFile) {
    final StringBuilder stableKey = new StringBuilder();
    appendStablePart(stableKey, sinkTaskId);
    appendStablePart(stableKey, databaseName);
    appendStablePart(stableKey, Integer.toString(outputIndex));
    appendStablePart(stableKey, Boolean.toString(hasModFile));

    final List<String> eventIdentities = new ArrayList<>();
    if (events != null) {
      for (final EnrichedEvent event : events) {
        if (event == null) {
          continue;
        }
        final StringBuilder eventIdentity = new StringBuilder();
        appendStablePart(eventIdentity, event.getClass().getName());
        appendStablePart(eventIdentity, event.getPipeName());
        appendStablePart(eventIdentity, Long.toString(event.getCreationTime()));
        appendStablePart(eventIdentity, Integer.toString(event.getRegionId()));
        if (event.getCommitterKey() != null) {
          appendStablePart(eventIdentity, event.getCommitterKey().getPipeName());
          appendStablePart(eventIdentity, Long.toString(event.getCommitterKey().getCreationTime()));
          appendStablePart(eventIdentity, Integer.toString(event.getCommitterKey().getRegionId()));
          appendStablePart(
              eventIdentity, Integer.toString(event.getCommitterKey().getRestartTimes()));
        }
        final List<Long> commitIds = new ArrayList<>();
        try {
          if (event.getCommitIds() != null) {
            commitIds.addAll(event.getCommitIds());
          }
        } catch (final UnsupportedOperationException ignored) {
          // A legacy event may not expose commit ids. The remaining stable fields still identify
          // it.
        }
        commitIds.sort(Comparator.naturalOrder());
        for (final long commitId : commitIds) {
          appendStablePart(eventIdentity, Long.toString(commitId));
        }
        try {
          appendStablePart(eventIdentity, Integer.toString(event.getRebootTimes()));
        } catch (final UnsupportedOperationException ignored) {
          appendStablePart(eventIdentity, UNSUPPORTED_REBOOT_TIMES);
        }
        try {
          appendStablePart(eventIdentity, Long.toString(event.getReplicateIndexForIoTV2()));
        } catch (final UnsupportedOperationException ignored) {
          appendStablePart(eventIdentity, UNSUPPORTED_REPLICATE_INDEX);
        }
        if (event.getCommitterKey() == null) {
          try {
            appendStablePart(eventIdentity, String.valueOf(event.getProgressIndex()));
          } catch (final UnsupportedOperationException ignored) {
            appendStablePart(eventIdentity, UNSUPPORTED_PROGRESS_INDEX);
          }
        }
        eventIdentities.add(eventIdentity.toString());
      }
    }
    eventIdentities.sort(Comparator.naturalOrder());
    appendStablePart(stableKey, Integer.toString(eventIdentities.size()));
    eventIdentities.forEach(identity -> appendStablePart(stableKey, identity));
    return UUID.nameUUIDFromBytes(stableKey.toString().getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static String generateConversionTaskId(
      final String sinkTaskId,
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String fileIdentity) {
    final StringBuilder stableKey = new StringBuilder();
    appendStablePart(stableKey, sinkTaskId);
    appendStablePart(stableKey, modFileName);
    appendStablePart(stableKey, Long.toString(modFileLength));
    appendStablePart(stableKey, tsFileName);
    appendStablePart(stableKey, Long.toString(tsFileLength));
    appendStablePart(stableKey, fileIdentity);
    appendStablePart(stableKey, Boolean.toString(modFileName != null));
    return UUID.nameUUIDFromBytes(stableKey.toString().getBytes(StandardCharsets.UTF_8)).toString();
  }

  private static void appendStablePart(final StringBuilder builder, final String value) {
    final String normalizedValue = value == null ? "" : value;
    builder.append(normalizedValue.length()).append(':').append(normalizedValue).append('\0');
  }

  protected static String generateDatabaseNameWithFileNameKey(final String fileName) {
    return DATABASE_NAME_KEY_PREFIX + fileName;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength)
      throws IOException {
    return toTPipeTransferReq(modFileName, modFileLength, tsFileName, tsFileLength, null);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    return toTPipeTransferReq(
        modFileName, modFileLength, tsFileName, tsFileLength, dataBaseName, false);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq()
            .convertToTPipeTransferReq(
                Arrays.asList(modFileName, tsFileName),
                Arrays.asList(modFileLength, tsFileLength),
                generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String tsFileName, final long tsFileLength, final String dataBaseName)
      throws IOException {
    return toTPipeTransferReq(tsFileName, tsFileLength, dataBaseName, false);
  }

  public static PipeTransferTsFileSealWithModReq toTPipeTransferReq(
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq()
            .convertToTPipeTransferReq(
                Collections.singletonList(tsFileName),
                Collections.singletonList(tsFileLength),
                generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static PipeTransferTsFileSealWithModReq fromTPipeTransferReq(final TPipeTransferReq req) {
    return (PipeTransferTsFileSealWithModReq)
        new PipeTransferTsFileSealWithModReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength)
      throws IOException {
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Arrays.asList(modFileName, tsFileName),
            Arrays.asList(modFileLength, tsFileLength),
            new HashMap<>());
  }

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName)
      throws IOException {
    return toTPipeTransferBytes(
        modFileName, modFileLength, tsFileName, tsFileLength, dataBaseName, false);
  }

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Arrays.asList(modFileName, tsFileName),
            Arrays.asList(modFileLength, tsFileLength),
            generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  public static byte[] toTPipeTransferBytes(
      final String modFileName,
      final long modFileLength,
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad,
      final String conversionTaskId,
      final boolean asyncLoadOnTypeMismatch)
      throws IOException {
    final HashMap<String, String> parameters =
        generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad);
    if (conversionTaskId != null) {
      parameters.put(CONVERSION_TASK_ID_KEY, conversionTaskId);
    }
    parameters.put(ASYNC_LOAD_ON_TYPE_MISMATCH_KEY, Boolean.toString(asyncLoadOnTypeMismatch));
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Arrays.asList(modFileName, tsFileName),
            Arrays.asList(modFileLength, tsFileLength),
            parameters);
  }

  public static byte[] toTPipeTransferBytes(
      final String tsFileName, final long tsFileLength, final String dataBaseName)
      throws IOException {
    return toTPipeTransferBytes(tsFileName, tsFileLength, dataBaseName, false);
  }

  public static byte[] toTPipeTransferBytes(
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad,
      final String conversionTaskId,
      final boolean asyncLoadOnTypeMismatch)
      throws IOException {
    final HashMap<String, String> parameters =
        generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad);
    if (conversionTaskId != null) {
      parameters.put(CONVERSION_TASK_ID_KEY, conversionTaskId);
    }
    parameters.put(ASYNC_LOAD_ON_TYPE_MISMATCH_KEY, Boolean.toString(asyncLoadOnTypeMismatch));
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Collections.singletonList(tsFileName),
            Collections.singletonList(tsFileLength),
            parameters);
  }

  public static byte[] toTPipeTransferBytes(
      final String tsFileName,
      final long tsFileLength,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad)
      throws IOException {
    return new PipeTransferTsFileSealWithModReq()
        .convertToTPipeTransferSnapshotSealBytes(
            Collections.singletonList(tsFileName),
            Collections.singletonList(tsFileLength),
            generateParameters(tsFileName, dataBaseName, shouldWaitForSchemaBeforeLoad));
  }

  private static HashMap<String, String> generateParameters(
      final String tsFileName,
      final String dataBaseName,
      final boolean shouldWaitForSchemaBeforeLoad) {
    final HashMap<String, String> parameters = new HashMap<>();
    parameters.put(generateDatabaseNameWithFileNameKey(tsFileName), dataBaseName);
    if (shouldWaitForSchemaBeforeLoad) {
      parameters.put(WAIT_FOR_SCHEMA_BEFORE_LOAD_KEY, Boolean.TRUE.toString());
    }
    return parameters;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof PipeTransferTsFileSealWithModReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
