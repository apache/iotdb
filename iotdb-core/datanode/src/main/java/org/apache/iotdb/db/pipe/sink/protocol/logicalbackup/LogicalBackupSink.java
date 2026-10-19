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

package org.apache.iotdb.db.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecordType;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupSettings;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionSnapshotEvent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferPlanNodeReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletInsertNodeReqV2;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.request.PipeTransferTabletRawReqV2;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.write.record.Tablet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

@TreeModel
@TableModel
public class LogicalBackupSink implements PipeConnector {

  private static final int FILE_PIECE_BYTES = 64 * 1024;
  protected LogicalBackupWriter writer;
  private String pipeName;
  private long creationTime;
  private int regionId;
  protected boolean includeHeartbeat;
  protected String unsupportedEventPolicy;
  private final String streamType;
  private long backupStartTime = Long.MIN_VALUE;
  private long backupEndTime = Long.MAX_VALUE;

  public LogicalBackupSink() {
    this("unknown");
  }

  public LogicalBackupSink(final String streamType) {
    this.streamType = streamType;
  }

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();
    validator.validateSynonymAttributes(
        Collections.singletonList(PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_DIR_KEY),
        Collections.singletonList(PipeSinkConstant.SINK_LOGICAL_BACKUP_DIR_KEY),
        true);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_ID_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_ID_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_SEGMENT_SIZE_BYTES_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_SEGMENT_SIZE_BYTES_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_MAX_RECORD_BYTES_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_MAX_RECORD_BYTES_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_FSYNC_POLICY_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_FSYNC_POLICY_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_FSYNC_BATCH_OPERATIONS_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_FSYNC_BATCH_OPERATIONS_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_FSYNC_PERIOD_MS_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_FSYNC_PERIOD_MS_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_UNSUPPORTED_EVENT_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_UNSUPPORTED_EVENT_KEY);
    validateSynonymAttributes(
        validator,
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_INCLUDE_HEARTBEAT_KEY,
        PipeSinkConstant.SINK_LOGICAL_BACKUP_INCLUDE_HEARTBEAT_KEY);
    validator.validateAttributeValueRange(
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
        true,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_FAIL_IF_EXISTS,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_NEW);
    validator.validateAttributeValueRange(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY,
        true,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_FAIL_IF_EXISTS,
        PipeSinkConstant.LOGICAL_BACKUP_RESUME_NEW);
    validator.validateAttributeValueRange(
        PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_FSYNC_POLICY_KEY,
        true,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_ALWAYS,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_BATCH,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_PERIODIC,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_NONE);
    validator.validateAttributeValueRange(
        PipeSinkConstant.SINK_LOGICAL_BACKUP_FSYNC_POLICY_KEY,
        true,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_ALWAYS,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_BATCH,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_PERIODIC,
        PipeSinkConstant.LOGICAL_BACKUP_FSYNC_NONE);
    validateSharedDirectoryResumePolicy(parameters);
    new LogicalBackupSettings(parameters, "backup");
    final String policy =
        parameters
            .getStringOrDefault(
                Arrays.asList(
                    PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_UNSUPPORTED_EVENT_KEY,
                    PipeSinkConstant.SINK_LOGICAL_BACKUP_UNSUPPORTED_EVENT_KEY),
                PipeSinkConstant.LOGICAL_BACKUP_UNSUPPORTED_EVENT_DEFAULT_VALUE)
            .toLowerCase(Locale.ROOT);
    if (!PipeSinkConstant.LOGICAL_BACKUP_UNSUPPORTED_EVENT_FAIL.equals(policy)
        && !PipeSinkConstant.LOGICAL_BACKUP_UNSUPPORTED_EVENT_SKIP.equals(policy)) {
      throw new PipeParameterNotValidException(
          String.format(
              DataNodePipeMessages.EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EVENT_POLICY_ARG_BF42E405,
              policy));
    }
  }

  private static void validateSynonymAttributes(
      final PipeParameterValidator validator,
      final String connectorAttribute,
      final String sinkAttribute) {
    validator.validateSynonymAttributes(
        Collections.singletonList(connectorAttribute),
        Collections.singletonList(sinkAttribute),
        false);
  }

  private void validateSharedDirectoryResumePolicy(final PipeParameters parameters)
      throws PipeParameterNotValidException {
    if (!"data".equals(streamType) && !"config".equals(streamType)) {
      return;
    }
    final boolean explicitlyConfigured =
        parameters.hasAnyAttributes(
            PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
            PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY);
    final String resume =
        parameters.getStringOrDefault(
            Arrays.asList(
                PipeSinkConstant.CONNECTOR_LOGICAL_BACKUP_RESUME_KEY,
                PipeSinkConstant.SINK_LOGICAL_BACKUP_RESUME_KEY),
            PipeSinkConstant.LOGICAL_BACKUP_RESUME_DEFAULT_VALUE);
    if (!explicitlyConfigured
        || !PipeSinkConstant.LOGICAL_BACKUP_RESUME_APPEND.equalsIgnoreCase(resume)) {
      throw new PipeParameterNotValidException(
          DataNodePipeMessages
              .EXCEPTION_CONFIGREGION_AND_DATAREGION_LOGICAL_BACKUP_REQUIRE_EXPLICIT_SINK_RESUME_APPEND_OR_CONNECTOR_RESUME_APPEND_AND_A_SHARED_BACKUP_DIRECTORY_B041371B);
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    pipeName = configuration.getRuntimeEnvironment().getPipeName();
    creationTime = configuration.getRuntimeEnvironment().getCreationTime();
    regionId = configuration.getRuntimeEnvironment().getRegionId();
    validateSharedDirectoryResumePolicy(parameters);
    final LogicalBackupSettings settings =
        new LogicalBackupSettings(parameters, pipeName + "-" + creationTime);
    includeHeartbeat = settings.includeHeartbeat;
    unsupportedEventPolicy = settings.unsupported;
    if (settings.captureOptions.containsKey("start-time")) {
      backupStartTime =
          DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
              settings.captureOptions.get("start-time"));
    }
    if (settings.captureOptions.containsKey("end-time")) {
      backupEndTime =
          DateTimeUtils.convertTimestampOrDatetimeStrToLongWithDefaultZone(
              settings.captureOptions.get("end-time"));
    }
    if (configuration.getRuntimeEnvironment() instanceof PipeTaskTemporaryRuntimeEnvironment) {
      return;
    }
    final String streamId = pipeName + "-" + creationTime + "-" + streamType + "-" + regionId;
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = settings.backupId;
    manifest.pipeName = pipeName;
    manifest.pipeCreationTime = creationTime;
    manifest.sourceClusterId = getSourceClusterId();
    manifest.sourceVersion = IoTDBConstant.VERSION;
    manifest.timestampPrecision =
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    manifest.streamId = streamId;
    manifest.regionId = regionId;
    manifest.streamType = streamType;
    manifest.sinkTaskId = streamId;
    writer = settings.open(streamType, regionId, manifest);
  }

  protected String getSourceClusterId() {
    return IoTDBDescriptor.getInstance().getConfig().getClusterId();
  }

  @Override
  public void handshake() {}

  @Override
  public void heartbeat() throws Exception {
    if (writer != null) {
      writer.heartbeat();
    }
  }

  @Override
  public void transfer(final TabletInsertionEvent event) throws Exception {
    if (event instanceof PipeInsertNodeTabletInsertionEvent) {
      final PipeInsertNodeTabletInsertionEvent insert = (PipeInsertNodeTabletInsertionEvent) event;
      transferEnriched(insert, () -> requests(insert));
    } else if (event instanceof PipeRawTabletInsertionEvent) {
      final PipeRawTabletInsertionEvent raw = (PipeRawTabletInsertionEvent) event;
      transferEnriched(
          raw,
          () ->
              Collections.singletonList(
                  PipeTransferTabletRawReqV2.toTPipeTransferReq(
                      raw.convertToTablet(),
                      raw.isAligned(),
                      raw.isTableModelEvent()
                          ? raw.getTableModelDatabaseName()
                          : raw.getTreeModelDatabaseName())));
    } else {
      handleUnsupported(event);
    }
  }

  @Override
  public void transfer(final TsFileInsertionEvent event) throws Exception {
    if (!(event instanceof PipeTsFileInsertionEvent)) {
      handleUnsupported(event);
      return;
    }
    final PipeTsFileInsertionEvent tsFile = (PipeTsFileInsertionEvent) event;
    final List<TPipeTransferReq> requests = new ArrayList<>();
    if (!tsFile.increaseReferenceCount(getClass().getName())) {
      return;
    }
    try {
      tsFile.consumeTabletInsertionEventsWithRetry(
          tablet -> {
            try {
              requests.add(
                  PipeTransferTabletRawReqV2.toTPipeTransferReq(
                      tablet.convertToTablet(),
                      tablet.isAligned(),
                      tablet.isTableModelEvent()
                          ? tablet.getTableModelDatabaseName()
                          : tablet.getTreeModelDatabaseName()));
            } catch (final IOException e) {
              throw new PipeException(e.getMessage(), e);
            }
          },
          getClass().getName());
      if (!requests.isEmpty()) {
        writer.writeEvent(eventId(tsFile), System.currentTimeMillis(), requests, metadata(tsFile));
      }
    } finally {
      event.close();
      tsFile.decreaseReferenceCount(getClass().getName(), false);
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeDeleteDataNodeEvent) {
      final PipeDeleteDataNodeEvent deletion = (PipeDeleteDataNodeEvent) event;
      transferEnriched(
          deletion,
          () -> {
            final Optional<AbstractDeleteDataNode> node =
                boundedDeletion(deletion.getDeleteDataNode(), backupStartTime, backupEndTime);
            return node.isPresent()
                ? Collections.singletonList(PipeTransferPlanNodeReq.toTPipeTransferReq(node.get()))
                : Collections.emptyList();
          });
    } else if (event instanceof PipeSchemaRegionWritePlanEvent) {
      final PipeSchemaRegionWritePlanEvent schema = (PipeSchemaRegionWritePlanEvent) event;
      transferEnriched(
          schema,
          () ->
              Collections.singletonList(
                  PipeTransferPlanNodeReq.toTPipeTransferReq(schema.getPlanNode())));
    } else if (event instanceof PipeSchemaRegionSnapshotEvent) {
      transferSnapshot((PipeSchemaRegionSnapshotEvent) event);
    } else if (event instanceof PipeHeartbeatEvent) {
      if (includeHeartbeat) {
        writer.writeControl(
            LogicalBackupRecordType.HEARTBEAT, System.currentTimeMillis(), metadata(event));
      }
    } else if (event instanceof PipeTerminateEvent) {
      // The framework commits this ordered completion signal. close() writes the final STREAM_END;
      // a continuous source also emits a historical completion signal before its realtime events.
    } else {
      handleUnsupported(event);
    }
  }

  private void transferSnapshot(final PipeSchemaRegionSnapshotEvent event) throws Exception {
    if (!event.increaseReferenceCount(getClass().getName())) {
      return;
    }
    try {
      final List<TPipeTransferReq> requests = new ArrayList<>();
      final File[] files =
          new File[] {
            event.getMTreeSnapshotFile(),
            event.getTagLogSnapshotFile(),
            event.getAttributeSnapshotFile()
          };
      for (final File file : files) {
        if (file == null) {
          continue;
        }
        try (final FileInputStream input = new FileInputStream(file)) {
          final byte[] piece = new byte[FILE_PIECE_BYTES];
          long offset = 0;
          int length;
          while ((length = input.read(piece)) >= 0) {
            if (length == 0) {
              continue;
            }
            requests.add(
                PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(
                    file.getName(), offset, Arrays.copyOf(piece, length)));
            offset += length;
          }
        }
      }
      requests.add(
          PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(
              event.getTreePattern().getPattern(),
              event.getTablePattern().getDatabasePattern(),
              event.getTablePattern().getTablePattern(),
              event.getTreePattern().isTreeModelDataAllowedToBeCaptured(),
              event.getTablePattern().isTableModelDataAllowedToBeCaptured(),
              files[0] == null ? null : files[0].getName(),
              files[0] == null ? 0 : files[0].length(),
              files[1] == null ? null : files[1].getName(),
              files[1] == null ? 0 : files[1].length(),
              files[2] == null ? null : files[2].getName(),
              files[2] == null ? 0 : files[2].length(),
              event.getDatabaseName(),
              event.toSealTypeString()));
      writer.writeEvent(eventId(event), System.currentTimeMillis(), requests, metadata(event));
    } finally {
      event.decreaseReferenceCount(getClass().getName(), false);
    }
  }

  List<TPipeTransferReq> requests(final PipeInsertNodeTabletInsertionEvent event)
      throws IOException {
    final String databaseName =
        event.isTableModelEvent()
            ? event.getTableModelDatabaseName()
            : event.getTreeModelDatabaseName();
    if (!(event.getInsertNode() instanceof RelationalInsertTabletNode)
        && !event.shouldParseTimeOrPattern()) {
      return Collections.singletonList(
          PipeTransferTabletInsertNodeReqV2.toTPipeTransferReq(
              event.getInsertNode(), databaseName));
    }

    final List<Tablet> tablets = event.convertToTablets();
    final List<TPipeTransferReq> requests = new ArrayList<>(tablets.size());
    for (int index = 0; index < tablets.size(); index++) {
      if (tablets.get(index).getRowSize() == 0) {
        continue;
      }
      requests.add(
          PipeTransferTabletRawReqV2.toTPipeTransferReq(
              tablets.get(index), event.isAligned(index), databaseName));
    }
    return requests;
  }

  static Optional<AbstractDeleteDataNode> boundedDeletion(
      final AbstractDeleteDataNode node, final long start, final long end) {
    if (node instanceof DeleteDataNode) {
      final DeleteDataNode tree = (DeleteDataNode) node;
      final long first = Math.max(start, tree.getDeleteStartTime());
      final long last = Math.min(end, tree.getDeleteEndTime());
      return first > last
          ? Optional.empty()
          : Optional.of(new DeleteDataNode(tree.getPlanNodeId(), tree.getPathList(), first, last));
    }
    if (node instanceof RelationalDeleteDataNode) {
      final RelationalDeleteDataNode table = (RelationalDeleteDataNode) node;
      final List<TableDeletionEntry> entries = new ArrayList<>();
      for (final TableDeletionEntry entry : table.getModEntries()) {
        final long first = Math.max(start, entry.getStartTime());
        final long last = Math.min(end, entry.getEndTime());
        if (first <= last) {
          entries.add(new TableDeletionEntry(entry.getPredicate(), new TimeRange(first, last)));
        }
      }
      return entries.isEmpty()
          ? Optional.empty()
          : Optional.of(
              new RelationalDeleteDataNode(
                  table.getPlanNodeId(), entries, table.getDatabaseName()));
    }
    throw new IllegalArgumentException(
        String.format(
            DataNodePipeMessages.EXCEPTION_UNSUPPORTED_PIPE_EVENT_FOR_LOGICAL_BACKUP_ARG_521AEE97,
            node.getClass().getName()));
  }

  protected void transferEnriched(final EnrichedEvent event, final RequestSupplier requestSupplier)
      throws Exception {
    if (!event.increaseReferenceCount(getClass().getName())) {
      return;
    }
    try {
      final List<TPipeTransferReq> requests = requestSupplier.get();
      if (!requests.isEmpty()) {
        writer.writeEvent(eventId(event), System.currentTimeMillis(), requests, metadata(event));
      }
    } finally {
      event.decreaseReferenceCount(getClass().getName(), false);
    }
  }

  @FunctionalInterface
  protected interface RequestSupplier {
    List<TPipeTransferReq> get() throws Exception;
  }

  protected void handleUnsupported(final Event event) throws Exception {
    if (PipeSinkConstant.LOGICAL_BACKUP_UNSUPPORTED_EVENT_SKIP.equals(unsupportedEventPolicy)) {
      writer.recordSkippedEvent(
          System.currentTimeMillis(), event == null ? "null" : event.getClass().getName());
      return;
    }
    throw new PipeParameterNotValidException(
        String.format(
            DataNodePipeMessages.EXCEPTION_UNSUPPORTED_PIPE_EVENT_FOR_LOGICAL_BACKUP_ARG_521AEE97,
            event == null ? "null" : event.getClass().getName()));
  }

  protected UUID eventId(final EnrichedEvent event) {
    return UUID.nameUUIDFromBytes(
        (event.getClass().getName()
                + '\0'
                + event.getPipeName()
                + '\0'
                + event.getCreationTime()
                + '\0'
                + event.getRegionId()
                + '\0'
                + commitIds(event))
            .getBytes(StandardCharsets.UTF_8));
  }

  private String commitIds(final EnrichedEvent event) {
    try {
      return String.valueOf(event.getCommitIds());
    } catch (final UnsupportedOperationException ignored) {
      return Long.toString(event.getCommitId());
    }
  }

  protected String metadata(final Event event) {
    if (event instanceof EnrichedEvent) {
      final EnrichedEvent enriched = (EnrichedEvent) event;
      return "pipe="
          + enriched.getPipeName()
          + ";creationTime="
          + enriched.getCreationTime()
          + ";regionId="
          + enriched.getRegionId()
          + ";commitIds="
          + commitIds(enriched)
          + ";eventType="
          + event.getClass().getName();
    }
    return "eventType=" + (event == null ? "null" : event.getClass().getName());
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }
}
