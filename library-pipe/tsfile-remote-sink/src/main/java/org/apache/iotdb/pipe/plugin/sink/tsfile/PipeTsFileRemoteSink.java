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

package org.apache.iotdb.pipe.plugin.sink.tsfile;

import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.sink.protocol.PipeBatchMetricsSettable;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.terminate.PipeTerminateEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.util.PipeObjectPathUtil;
import org.apache.iotdb.db.pipe.sink.payload.evolvable.batch.PipeTabletEventTsFileBatch;
import org.apache.iotdb.db.pipe.sink.util.builder.TsFileNameGenerator;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.metrics.impl.DoNothingHistogram;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.pipe.api.PipeSink;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeSinkRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_TS_FILE_BATCH_DELAY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_TS_FILE_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_REMOTE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SCP_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_REMOTE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SCP_USER_KEY;

@TreeModel
@TableModel
public class PipeTsFileRemoteSink implements PipeSink, PipeBatchMetricsSettable {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileRemoteSink.class);
  private final TsFileNameGenerator tsFileNameGenerator = new TsFileNameGenerator();
  private RemoteFileTransfer remoteFileTransfer;
  private PipeTabletEventTsFileBatch eventTsFileBatch;

  private Histogram tsFileBatchSizeHistogram = new DoNothingHistogram();
  private Histogram tsFileBatchTimeIntervalHistogram = new DoNothingHistogram();
  private Histogram eventSizeHistogram = new DoNothingHistogram();

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    this.remoteFileTransfer = new ScpRemoteFileTransfer(parameters);

    final Integer requestMaxDelayInMillis =
        parameters.getIntByKeys(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, SINK_IOTDB_BATCH_DELAY_MS_KEY);
    int requestMaxDelayInMs;
    long requestMaxBatchSizeInBytes;
    if (Objects.isNull(requestMaxDelayInMillis)) {
      final int requestMaxDelayConfig =
          parameters.getIntOrDefault(
              Arrays.asList(
                  CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY, SINK_IOTDB_BATCH_DELAY_SECONDS_KEY),
              CONNECTOR_IOTDB_TS_FILE_BATCH_DELAY_DEFAULT_VALUE * 1000);
      requestMaxDelayInMs = requestMaxDelayConfig < 0 ? Integer.MAX_VALUE : requestMaxDelayConfig;
    } else {
      requestMaxDelayInMs =
          requestMaxDelayInMillis < 0 ? Integer.MAX_VALUE : requestMaxDelayInMillis;
    }
    requestMaxBatchSizeInBytes =
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_TS_FILE_BATCH_SIZE_DEFAULT_VALUE);
    this.eventTsFileBatch =
        new PipeTabletEventTsFileBatch(
            requestMaxDelayInMs, requestMaxBatchSizeInBytes, this::recordTsFileMetric);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeSinkRuntimeConfiguration configuration)
      throws Exception {
    customize(parameters, (PipeConnectorRuntimeConfiguration) configuration);
  }

  @Override
  public void handshake() throws Exception {
    if (remoteFileTransfer != null) {
      remoteFileTransfer.handshake();
    }
  }

  @Override
  public void heartbeat() throws Exception {
    if (remoteFileTransfer == null || eventTsFileBatch == null) {
      return;
    }
    transferBatchedTsFilesIfNecessary();
  }

  @Override
  public void transfer(final TabletInsertionEvent tabletInsertionEvent) throws Exception {
    eventTsFileBatch.onEvent(tabletInsertionEvent);
    transferBatchedTsFilesIfNecessary();
  }

  @Override
  public void transfer(final TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    if (remoteFileTransfer == null) {
      return;
    }
    if (tsFileInsertionEvent instanceof PipeTsFileInsertionEvent) {
      final PipeTsFileInsertionEvent event = (PipeTsFileInsertionEvent) tsFileInsertionEvent;
      if (!event.waitForTsFileClose()) {
        throw new PipeException(
            "Timeout waiting for tsfile close before sink transfer: " + tsFileInsertionEvent);
      }
      final TsFileResource tsFileResource = event.getTsFileResource();
      if (tsFileResource == null) {
        throw new PipeException("TsFile resource is null, event: " + tsFileInsertionEvent);
      }
      final File tsFile = tsFileResource.getTsFile();
      if (tsFile != null && tsFile.exists()) {
        remoteFileTransfer.transferFile(
            tsFile,
            PipeObjectPathUtil.resolveLinkedObjectDirectory(tsFileResource, event.getPipeName()),
            tsFileNameGenerator.nextFileName());
      }
    } else {
      remoteFileTransfer.transferFile(
          tsFileInsertionEvent.getTsFile(), null, tsFileNameGenerator.nextFileName());
    }
  }

  @Override
  public void transfer(final Event event) throws Exception {
    if (event instanceof TsFileInsertionEvent) {
      transfer((TsFileInsertionEvent) event);
    } else if (event instanceof PipeHeartbeatEvent || event instanceof PipeTerminateEvent) {
      heartbeat();
    } else {
      LOGGER.warn("PipeTsFileRemoteSink does not support transferring generic event: {}.", event);
    }
  }

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters p = validator.getParameters();
    final String host = p.getStringByKeys(CONNECTOR_SCP_HOST_KEY, SINK_SCP_HOST_KEY);
    final String user = p.getStringByKeys(CONNECTOR_SCP_USER_KEY, SINK_SCP_USER_KEY);
    final String remotePath =
        p.getStringByKeys(CONNECTOR_SCP_REMOTE_PATH_KEY, SINK_SCP_REMOTE_PATH_KEY);
    final String mode =
        p.getStringByKeys(
            PipeSinkConstant.CONNECTOR_FILE_MODE_KEY, PipeSinkConstant.SINK_FILE_MODE_KEY);
    if (mode != null && !mode.trim().isEmpty()) {
      throw new PipeParameterNotValidException(
          "tsfile-remote-sink does not require sink.file-mode/connector.file-mode.");
    }
    validator.validate(
        arg -> arg instanceof String && !((String) arg).trim().isEmpty(),
        "sink.scp.host (or connector.scp.host) is required for tsfile-remote-sink.",
        host);
    validator.validate(
        arg -> arg instanceof String && !((String) arg).trim().isEmpty(),
        "sink.scp.user (or connector.scp.user) is required for tsfile-remote-sink.",
        user);
    validator.validate(
        arg -> arg instanceof String && !((String) arg).trim().isEmpty(),
        "sink.scp.remote-path (or connector.scp.remote-path) is required for tsfile-remote-sink.",
        remotePath);
  }

  @Override
  public void close() throws Exception {
    if (remoteFileTransfer != null) {
      remoteFileTransfer.close();
      remoteFileTransfer = null;
    }
  }

  @Override
  public void setTabletBatchSizeHistogram(Histogram tabletBatchSizeHistogram) {
    // tsfile-remote-sink does not emit tablet batches.
  }

  @Override
  public void setTsFileBatchSizeHistogram(Histogram tsFileBatchSizeHistogram) {
    if (tsFileBatchSizeHistogram != null) {
      this.tsFileBatchSizeHistogram = tsFileBatchSizeHistogram;
    }
  }

  @Override
  public void setTabletBatchTimeIntervalHistogram(Histogram tabletBatchTimeIntervalHistogram) {
    // tsfile-remote-sink does not emit tablet batches.
  }

  @Override
  public void setTsFileBatchTimeIntervalHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    if (tsFileBatchTimeIntervalHistogram != null) {
      this.tsFileBatchTimeIntervalHistogram = tsFileBatchTimeIntervalHistogram;
    }
  }

  @Override
  public void setBatchEventSizeHistogram(Histogram eventSizeHistogram) {
    if (eventSizeHistogram != null) {
      this.eventSizeHistogram = eventSizeHistogram;
    }
  }

  public void recordTsFileMetric(long timeInterval, long bufferSize, long eventSize) {
    this.tsFileBatchTimeIntervalHistogram.update(timeInterval);
    this.tsFileBatchSizeHistogram.update(bufferSize);
    this.eventSizeHistogram.update(eventSize);
  }

  private void transferBatchedTsFilesIfNecessary() throws Exception {
    if (!eventTsFileBatch.shouldEmit() || eventTsFileBatch.isEmpty()) {
      return;
    }
    final List<Pair<String, Pair<File, File>>> list = eventTsFileBatch.sealTsFiles();
    for (final Pair<String, Pair<File, File>> sealed : list) {
      final Pair<File, File> tsFileAndObjectDir = sealed.getRight();
      if (tsFileAndObjectDir == null) {
        continue;
      }
      final File tsFile = tsFileAndObjectDir.getLeft();
      final File objectDir = tsFileAndObjectDir.getRight();
      if (tsFile != null && tsFile.exists()) {
        remoteFileTransfer.transferFile(tsFile, objectDir, tsFileNameGenerator.nextFileName());
      }
    }
    eventTsFileBatch.decreaseEventsReferenceCount(PipeTsFileRemoteSink.class.getName(), true);
    eventTsFileBatch.onSuccess();
  }
}
