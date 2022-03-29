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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.mpp.rpc.thrift.EndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.GetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.NewDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

public class DataBlockManager implements IDataBlockManager {

  private static final Logger logger = LoggerFactory.getLogger(DataBlockManager.class);

  public interface SourceHandleListener {
    void onFinished(SourceHandle sourceHandle);

    void onClosed(SourceHandle sourceHandle);
  }

  public interface SinkHandleListener {
    void onFinish(SinkHandle sinkHandle);

    void onClosed(SinkHandle sinkHandle);

    void onAborted(SinkHandle sinkHandle);
  }

  /** Handle thrift communications. */
  class DataBlockServiceImpl implements DataBlockService.Iface {

    public GetDataBlockResponse getDataBlock(GetDataBlockRequest req) throws TException {
      logger.debug(
          "Get data block request received. Asking for data block [{}, {}) from {}.",
          req.getStartSequenceId(),
          req.getEndSequenceId(),
          req.getSourceFragnemtInstanceId());
      if (!sinkHandles.containsKey(req.getSourceFragnemtInstanceId())) {
        throw new TException(
            "Source fragment instance not found. Fragment instance ID: "
                + req.getSourceFragnemtInstanceId()
                + ".");
      }
      GetDataBlockResponse resp = new GetDataBlockResponse();
      for (int i = req.getStartSequenceId(); i < req.getEndSequenceId(); i++) {
        ByteBuffer serializedTsBlock =
            sinkHandles
                .get(req.getSourceFragnemtInstanceId())
                .getSerializedTsBlock(req.getStartSequenceId());
        resp.addToTsBlocks(serializedTsBlock);
      }
      return resp;
    }

    @Override
    public void onNewDataBlockEvent(NewDataBlockEvent e) throws TException {
      logger.debug(
          "New data block event received, for operator {} of {} from {}.",
          e.getTargetOperatorId(),
          e.getTargetFragmentInstanceId(),
          e.getSourceFragnemtInstanceId());
      if (!sourceHandles.containsKey(e.getTargetFragmentInstanceId())
          || !sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .containsKey(e.getTargetOperatorId())
          || sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .get(e.getTargetOperatorId())
              .isClosed()) {
        logger.info("{} is ignored because the source handle does not exist or has been closed", e);
        return;
      }

      SourceHandle sourceHandle =
          sourceHandles.get(e.getTargetFragmentInstanceId()).get(e.getTargetOperatorId());
      sourceHandle.updatePendingDataBlockInfo(e.getStartSequenceId(), e.getBlockSizes());
    }

    @Override
    public void onEndOfDataBlockEvent(EndOfDataBlockEvent e) throws TException {
      logger.debug(
          "End of data block event received, for operator {} of {} from {}.",
          e.getTargetOperatorId(),
          e.getTargetFragmentInstanceId(),
          e.getSourceFragnemtInstanceId());
      if (!sourceHandles.containsKey(e.getTargetFragmentInstanceId())
          || !sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .containsKey(e.getTargetOperatorId())
          || sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .get(e.getTargetOperatorId())
              .isClosed()) {
        logger.info("{} is ignored because the source handle does not exist or has been closed", e);
        return;
      }
      SourceHandle sourceHandle =
          sourceHandles
              .getOrDefault(e.getTargetFragmentInstanceId(), Collections.emptyMap())
              .get(e.getTargetOperatorId());
      sourceHandle.setNoMoreTsBlocks();
    }
  }

  /** Listen to the state changes of a source handle. */
  class SourceHandleListenerImpl implements SourceHandleListener {
    @Override
    public void onFinished(SourceHandle sourceHandle) {
      logger.info("Release resources of finished source handle {}", sourceHandle);
      if (!sourceHandles.containsKey(sourceHandle.getLocalFragmentInstanceId())
          || !sourceHandles
              .get(sourceHandle.getLocalFragmentInstanceId())
              .containsKey(sourceHandle.getLocalOperatorId())) {
        logger.info(
            "Resources of finished source handle {} has already been released", sourceHandle);
      }
      sourceHandles
          .get(sourceHandle.getLocalFragmentInstanceId())
          .remove(sourceHandle.getLocalOperatorId());
      if (sourceHandles.get(sourceHandle.getLocalFragmentInstanceId()).isEmpty()) {
        sourceHandles.remove(sourceHandle.getLocalFragmentInstanceId());
      }
    }

    @Override
    public void onClosed(SourceHandle sourceHandle) {
      onFinished(sourceHandle);
    }
  }

  /** Listen to the state changes of a sink handle. */
  class SinkHandleListenerImpl implements SinkHandleListener {

    @Override
    public void onFinish(SinkHandle sinkHandle) {
      logger.info("Release resources of finished sink handle {}", sourceHandles);
      if (!sinkHandles.containsKey(sinkHandle.getLocalFragmentInstanceId())) {
        logger.info("Resources of finished sink handle {} has already been released", sinkHandle);
      }
      sinkHandles.remove(sinkHandle.getLocalFragmentInstanceId());
    }

    @Override
    public void onClosed(SinkHandle sinkHandle) {}

    @Override
    public void onAborted(SinkHandle sinkHandle) {
      logger.info("Release resources of aborted sink handle {}", sourceHandles);
      if (!sinkHandles.containsKey(sinkHandle.getLocalFragmentInstanceId())) {
        logger.info("Resources of aborted sink handle {} has already been released", sinkHandle);
      }
      sinkHandles.remove(sinkHandle.getLocalFragmentInstanceId());
    }
  }

  private final LocalMemoryManager localMemoryManager;
  private final Supplier<TsBlockSerde> tsBlockSerdeFactory;
  private final ExecutorService executorService;
  private final DataBlockServiceClientFactory clientFactory;
  private final Map<TFragmentInstanceId, Map<String, SourceHandle>> sourceHandles;
  private final Map<TFragmentInstanceId, SinkHandle> sinkHandles;

  private DataBlockServiceImpl dataBlockService;

  public DataBlockManager(
      LocalMemoryManager localMemoryManager,
      Supplier<TsBlockSerde> tsBlockSerdeFactory,
      ExecutorService executorService,
      DataBlockServiceClientFactory clientFactory) {
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.tsBlockSerdeFactory = Validate.notNull(tsBlockSerdeFactory);
    this.executorService = Validate.notNull(executorService);
    this.clientFactory = Validate.notNull(clientFactory);
    sourceHandles = new ConcurrentHashMap<>();
    sinkHandles = new ConcurrentHashMap<>();
  }

  public DataBlockServiceImpl getOrCreateDataBlockServiceImpl() {
    if (dataBlockService != null) {
      dataBlockService = new DataBlockServiceImpl();
    }
    return dataBlockService;
  }

  @Override
  public ISinkHandle createSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String remoteHostname,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remoteOperatorId)
      throws IOException {
    if (sinkHandles.containsKey(localFragmentInstanceId)) {
      throw new IllegalStateException("Sink handle for " + localFragmentInstanceId + " exists.");
    }

    logger.info(
        "Create sink handle to operator {} of {} for {}",
        remoteOperatorId,
        remoteFragmentInstanceId,
        localFragmentInstanceId);

    SinkHandle sinkHandle =
        new SinkHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            remoteOperatorId,
            localFragmentInstanceId,
            localMemoryManager,
            executorService,
            clientFactory.getDataBlockServiceClient(remoteHostname, 7777),
            tsBlockSerdeFactory.get(),
            new SinkHandleListenerImpl());
    sinkHandles.put(localFragmentInstanceId, sinkHandle);
    return sinkHandle;
  }

  @Override
  public ISourceHandle createSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localOperatorId,
      String remoteHostname,
      TFragmentInstanceId remoteFragmentInstanceId)
      throws IOException {
    if (sourceHandles.containsKey(localFragmentInstanceId)
        && sourceHandles.get(localFragmentInstanceId).containsKey(localOperatorId)) {
      throw new IllegalStateException(
          "Source handle for operator "
              + localOperatorId
              + " of "
              + localFragmentInstanceId
              + " exists.");
    }

    logger.info(
        "Create source handle from {} for operator {} of {}",
        remoteFragmentInstanceId,
        localOperatorId,
        localFragmentInstanceId);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteHostname,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localOperatorId,
            localMemoryManager,
            executorService,
            // TODO: hard coded port.
            clientFactory.getDataBlockServiceClient(remoteHostname, 7777),
            tsBlockSerdeFactory.get(),
            new SourceHandleListenerImpl());
    sourceHandles
        .computeIfAbsent(localFragmentInstanceId, key -> new ConcurrentHashMap<>())
        .put(localOperatorId, sourceHandle);
    return sourceHandle;
  }

  /**
   * Release all the related resources, including data blocks that are not yet fetched by downstream
   * fragment instances.
   *
   * <p>This method should be called when a fragment instance finished in an abnormal state.
   */
  public void forceDeregisterFragmentInstance(TFragmentInstanceId fragmentInstanceId) {
    logger.info("Force deregister fragment instance {}", fragmentInstanceId);
    if (sinkHandles.containsKey(fragmentInstanceId)) {
      ISinkHandle sinkHandle = sinkHandles.get(fragmentInstanceId);
      logger.info("Abort sink handle {}", sinkHandle);
      sinkHandle.abort();
      sinkHandles.remove(fragmentInstanceId);
    }
    if (sourceHandles.containsKey(fragmentInstanceId)) {
      Map<String, SourceHandle> operatorIdToSourceHandle = sourceHandles.get(fragmentInstanceId);
      for (Entry<String, SourceHandle> entry : operatorIdToSourceHandle.entrySet()) {
        logger.info("Close source handle {}", sourceHandles);
        entry.getValue().close();
      }
      sourceHandles.remove(fragmentInstanceId);
    }
  }
}
