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
 */
package org.apache.iotdb.confignode.manager.pipe.sink.protocol.logicalbackup;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupManifest;
import org.apache.iotdb.commons.pipe.sink.logicalbackup.LogicalBackupWriter;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionSnapshotEvent;
import org.apache.iotdb.confignode.manager.pipe.event.PipeConfigRegionWritePlanEvent;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigPlanReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotPieceReq;
import org.apache.iotdb.confignode.manager.pipe.sink.payload.PipeTransferConfigSnapshotSealReq;
import org.apache.iotdb.confignode.service.ConfigNode;
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
import java.nio.file.Path;
import java.nio.file.Paths;
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

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator.validateSynonymAttributes(
        Collections.singletonList("connector.dir"), Collections.singletonList("sink.dir"), true);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final String pipeName = configuration.getRuntimeEnvironment().getPipeName();
    final long creationTime = configuration.getRuntimeEnvironment().getCreationTime();
    final int regionId = configuration.getRuntimeEnvironment().getRegionId();
    final String root = parameters.getStringByKeys("connector.dir", "sink.dir");
    final String backupId =
        parameters.getStringOrDefault(
            Arrays.asList("connector.backup-id", "sink.backup-id"), pipeName + "-" + creationTime);
    final Path directory =
        Paths.get(root)
            .toAbsolutePath()
            .normalize()
            .resolve(backupId)
            .resolve("config-" + regionId);
    final LogicalBackupManifest manifest = new LogicalBackupManifest();
    manifest.backupId = backupId;
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
    writer =
        new LogicalBackupWriter(
            directory,
            manifest,
            64 * 1024 * 1024,
            64 * 1024 * 1024,
            LogicalBackupWriter.FsyncPolicy.BATCH,
            100,
            1000,
            true);
  }

  @Override
  public void transfer(final TabletInsertionEvent event) throws Exception {}

  @Override
  public void transfer(final TsFileInsertionEvent event) throws Exception {}

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof PipeConfigRegionWritePlanEvent) {
      final PipeConfigRegionWritePlanEvent e = (PipeConfigRegionWritePlanEvent) event;
      if (e.increaseReferenceCount(getClass().getName())) {
        try {
          writer.writeEvent(
              UUID.randomUUID(),
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
          writer.writeEvent(
              UUID.randomUUID(), System.currentTimeMillis(), requests, "config-snapshot");
        } finally {
          e.decreaseReferenceCount(getClass().getName(), false);
        }
      }
    }
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
