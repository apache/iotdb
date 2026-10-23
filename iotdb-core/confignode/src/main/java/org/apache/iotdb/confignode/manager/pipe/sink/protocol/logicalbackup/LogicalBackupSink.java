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
package org.apache.iotdb.confignode.manager.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.i18n.LogicalBackupMessages;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskTemporaryRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupRecordType;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupSettings;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@TreeModel
@TableModel
public class LogicalBackupSink implements PipeConnector {
  private static final int FILE_PIECE_BYTES = 64 * 1024;
  private LogicalBackupWriter writer;
  private LogicalBackupSettings settings;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator.validateSynonymAttributes(
        Collections.singletonList("connector.dir"), Collections.singletonList("sink.dir"), true);
    final LogicalBackupSettings validated =
        new LogicalBackupSettings(validator.getParameters(), "backup");
    validated.requireSharedResume(validator.getParameters());
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final String pipeName = configuration.getRuntimeEnvironment().getPipeName();
    final long creationTime = configuration.getRuntimeEnvironment().getCreationTime();
    final int regionId = configuration.getRuntimeEnvironment().getRegionId();
    settings = new LogicalBackupSettings(parameters, pipeName + "-" + creationTime);
    settings.requireSharedResume(parameters);
    if (configuration.getRuntimeEnvironment() instanceof PipeTaskTemporaryRuntimeEnvironment) {
      return;
    }
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = settings.backupId;
    manifest.pipeName = pipeName;
    manifest.pipeCreationTime = creationTime;
    manifest.sourceClusterId =
        ConfigNode.getInstance().getConfigManager().getClusterManager().getClusterId();
    manifest.sourceVersion = IoTDBConstant.VERSION;
    manifest.timestampPrecision =
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
    manifest.streamId = pipeName + "-" + creationTime + "-config-" + regionId;
    manifest.regionId = regionId;
    manifest.streamType = "config";
    writer = settings.open("config", regionId, manifest);
  }

  @Override
  public void transfer(final TabletInsertionEvent event) throws Exception {
    unsupported(event);
  }

  @Override
  public void transfer(final TsFileInsertionEvent event) throws Exception {
    unsupported(event);
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeConfigRegionWritePlanEvent) {
      final PipeConfigRegionWritePlanEvent e = (PipeConfigRegionWritePlanEvent) event;
      if (e.increaseReferenceCount(getClass().getName())) {
        try {
          writer.writeEvent(
              eventId(e),
              System.currentTimeMillis(),
              Collections.singletonList(
                  PipeTransferConfigPlanReq.toTPipeTransferReq(e.getConfigPhysicalPlan())),
              "config-plan");
        } finally {
          e.decreaseReferenceCount(getClass().getName(), false);
        }
      }
    } else if (event instanceof PipeConfigRegionSnapshotEvent) {
      final PipeConfigRegionSnapshotEvent e = (PipeConfigRegionSnapshotEvent) event;
      if (e.increaseReferenceCount(getClass().getName())) {
        try {
          final List<TPipeTransferReq> requests = new ArrayList<>();
          appendFile(requests, e.getSnapshotFile());
          appendFile(requests, e.getTemplateFile());
          final File snapshot = e.getSnapshotFile();
          final File template = e.getTemplateFile();
          requests.add(
              PipeTransferConfigSnapshotSealReq.toTPipeTransferReq(
                  e.getTreePattern().getPattern(),
                  e.getTablePattern().getDatabasePattern(),
                  e.getTablePattern().getTablePattern(),
                  e.getTreePattern().isTreeModelDataAllowedToBeCaptured(),
                  e.getTablePattern().isTableModelDataAllowedToBeCaptured(),
                  snapshot.getName(),
                  snapshot.length(),
                  template == null ? null : template.getName(),
                  template == null ? 0 : template.length(),
                  e.getFileType(),
                  e.toSealTypeString(),
                  e.getAuthUserName()));
          writer.writeEvent(eventId(e), System.currentTimeMillis(), requests, "config-snapshot");
        } finally {
          e.decreaseReferenceCount(getClass().getName(), false);
        }
      }
    } else if (event instanceof PipeHeartbeatEvent) {
      if (settings.includeHeartbeat) {
        writer.writeControl(
            LogicalBackupRecordType.HEARTBEAT, System.currentTimeMillis(), "heartbeat");
      }
    } else if (event instanceof PipeTerminateEvent) {
      // close() writes STREAM_END after every preceding request has been committed.
    } else {
      unsupported(event);
    }
  }

  private void unsupported(final Event event) throws Exception {
    final String name = event == null ? "null" : event.getClass().getName();
    if ("skip".equals(settings.unsupported)) {
      writer.recordSkippedEvent(System.currentTimeMillis(), name);
    } else {
      throw new java.io.IOException(
          String.format(
              LogicalBackupMessages.EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_EVENT_ARG_30920E60, name));
    }
  }

  private static UUID eventId(final EnrichedEvent event) {
    String identity =
        event.getClass().getName()
            + "\0"
            + event.getPipeName()
            + "\0"
            + event.getCreationTime()
            + "\0"
            + event.getCommitId();
    if (event instanceof PipeConfigRegionSnapshotEvent) {
      final PipeConfigRegionSnapshotEvent snapshot = (PipeConfigRegionSnapshotEvent) event;
      identity += "\0" + snapshot.getFileType() + "\0" + snapshot.getSnapshotFile().getName();
    }
    return UUID.nameUUIDFromBytes(identity.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static void appendFile(final List<TPipeTransferReq> requests, final File file)
      throws Exception {
    if (file == null) return;
    try (FileInputStream input = new FileInputStream(file)) {
      final byte[] buffer = new byte[FILE_PIECE_BYTES];
      long offset = 0;
      int length;
      while ((length = input.read(buffer)) > 0) {
        requests.add(
            PipeTransferConfigSnapshotPieceReq.toTPipeTransferReq(
                file.getName(), offset, Arrays.copyOf(buffer, length)));
        offset += length;
      }
    }
  }

  @Override
  public void handshake() {}

  @Override
  public void heartbeat() throws Exception {
    if (writer != null) writer.heartbeat();
  }

  @Override
  public void close() throws Exception {
    if (writer != null) {
      writer.close();
      writer = null;
    }
  }
}
