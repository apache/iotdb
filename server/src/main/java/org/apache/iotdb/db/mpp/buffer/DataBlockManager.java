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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeDataBlockServiceClient;
import org.apache.iotdb.db.mpp.execution.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

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
    void onFinished(ISourceHandle sourceHandle);

    void onAborted(ISourceHandle sourceHandle);

    void onFailure(ISourceHandle sourceHandle, Throwable t);
  }

  public interface SinkHandleListener {
    void onFinish(ISinkHandle sinkHandle);

    void onEndOfBlocks(ISinkHandle sinkHandle);

    void onAborted(ISinkHandle sinkHandle);

    void onFailure(ISinkHandle sinkHandle, Throwable t);
  }

  /** Handle thrift communications. */
  class DataBlockServiceImpl implements DataBlockService.Iface {

    @Override
    public TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req) throws TException {
      logger.debug(
          "Get data block request received, for data blocks whose sequence ID in [{}, {}) from {}.",
          req.getStartSequenceId(),
          req.getEndSequenceId(),
          req.getSourceFragmentInstanceId());
      if (!sinkHandles.containsKey(req.getSourceFragmentInstanceId())) {
        throw new TException(
            "Source fragment instance not found. Fragment instance ID: "
                + req.getSourceFragmentInstanceId()
                + ".");
      }
      TGetDataBlockResponse resp = new TGetDataBlockResponse();
      SinkHandle sinkHandle = (SinkHandle) sinkHandles.get(req.getSourceFragmentInstanceId());
      for (int i = req.getStartSequenceId(); i < req.getEndSequenceId(); i++) {
        try {
          ByteBuffer serializedTsBlock = sinkHandle.getSerializedTsBlock(i);
          resp.addToTsBlocks(serializedTsBlock);
        } catch (IOException e) {
          throw new TException(e);
        }
      }
      return resp;
    }

    @Override
    public void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e) throws TException {
      logger.debug(
          "Acknowledge data block event received, for data blocks whose sequence ID in [{}, {}) from {}.",
          e.getStartSequenceId(),
          e.getEndSequenceId(),
          e.getSourceFragmentInstanceId());
      if (!sinkHandles.containsKey(e.getSourceFragmentInstanceId())) {
        throw new TException(
            "Source fragment instance not found. Fragment instance ID: "
                + e.getSourceFragmentInstanceId()
                + ".");
      }
      ((SinkHandle) sinkHandles.get(e.getSourceFragmentInstanceId()))
          .acknowledgeTsBlock(e.getStartSequenceId(), e.getEndSequenceId());
    }

    @Override
    public void onNewDataBlockEvent(TNewDataBlockEvent e) throws TException {
      logger.debug(
          "New data block event received, for plan node {} of {} from {}.",
          e.getTargetPlanNodeId(),
          e.getTargetFragmentInstanceId(),
          e.getSourceFragmentInstanceId());
      if (!sourceHandles.containsKey(e.getTargetFragmentInstanceId())
          || !sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .containsKey(e.getTargetPlanNodeId())
          || sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .get(e.getTargetPlanNodeId())
              .isAborted()) {
        throw new TException(
            "Target fragment instance not found. Fragment instance ID: "
                + e.getTargetFragmentInstanceId()
                + ".");
      }

      SourceHandle sourceHandle =
          (SourceHandle)
              sourceHandles.get(e.getTargetFragmentInstanceId()).get(e.getTargetPlanNodeId());
      sourceHandle.updatePendingDataBlockInfo(e.getStartSequenceId(), e.getBlockSizes());
    }

    @Override
    public void onEndOfDataBlockEvent(TEndOfDataBlockEvent e) throws TException {
      logger.debug(
          "End of data block event received, for plan node {} of {} from {}.",
          e.getTargetPlanNodeId(),
          e.getTargetFragmentInstanceId(),
          e.getSourceFragmentInstanceId());
      if (!sourceHandles.containsKey(e.getTargetFragmentInstanceId())
          || !sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .containsKey(e.getTargetPlanNodeId())
          || sourceHandles
              .get(e.getTargetFragmentInstanceId())
              .get(e.getTargetPlanNodeId())
              .isAborted()) {
        throw new TException(
            "Target fragment instance not found. Fragment instance ID: "
                + e.getTargetFragmentInstanceId()
                + ".");
      }
      SourceHandle sourceHandle =
          (SourceHandle)
              sourceHandles
                  .getOrDefault(e.getTargetFragmentInstanceId(), Collections.emptyMap())
                  .get(e.getTargetPlanNodeId());
      sourceHandle.setNoMoreTsBlocks(e.getLastSequenceId());
    }
  }

  /** Listen to the state changes of a source handle. */
  class SourceHandleListenerImpl implements SourceHandleListener {

    private final IDataBlockManagerCallback<Throwable> onFailureCallback;

    public SourceHandleListenerImpl(IDataBlockManagerCallback<Throwable> onFailureCallback) {
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinished(ISourceHandle sourceHandle) {
      logger.info("Release resources of finished source handle {}", sourceHandle);
      if (!sourceHandles.containsKey(sourceHandle.getLocalFragmentInstanceId())
          || !sourceHandles
              .get(sourceHandle.getLocalFragmentInstanceId())
              .containsKey(sourceHandle.getLocalPlanNodeId())) {
        logger.info(
            "Resources of finished source handle {} has already been released", sourceHandle);
      } else {
        sourceHandles
            .get(sourceHandle.getLocalFragmentInstanceId())
            .remove(sourceHandle.getLocalPlanNodeId());
      }
      if (sourceHandles.containsKey(sourceHandle.getLocalFragmentInstanceId())
          && sourceHandles.get(sourceHandle.getLocalFragmentInstanceId()).isEmpty()) {
        sourceHandles.remove(sourceHandle.getLocalFragmentInstanceId());
      }
    }

    @Override
    public void onAborted(ISourceHandle sourceHandle) {
      onFinished(sourceHandle);
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      logger.error("Source handle {} failed due to {}", sourceHandle, t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  /** Listen to the state changes of a sink handle. */
  class SinkHandleListenerImpl implements SinkHandleListener {

    private final FragmentInstanceContext context;
    private final IDataBlockManagerCallback<Throwable> onFailureCallback;

    public SinkHandleListenerImpl(
        FragmentInstanceContext context, IDataBlockManagerCallback<Throwable> onFailureCallback) {
      this.context = context;
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinish(ISinkHandle sinkHandle) {
      logger.info("Release resources of finished sink handle {}", sourceHandles);
      if (!sinkHandles.containsKey(sinkHandle.getLocalFragmentInstanceId())) {
        logger.info("Resources of finished sink handle {} has already been released", sinkHandle);
      }
      sinkHandles.remove(sinkHandle.getLocalFragmentInstanceId());
      context.finished();
    }

    @Override
    public void onEndOfBlocks(ISinkHandle sinkHandle) {
      context.transitionToFlushing();
    }

    @Override
    public void onAborted(ISinkHandle sinkHandle) {
      logger.info("Release resources of aborted sink handle {}", sourceHandles);
      if (!sinkHandles.containsKey(sinkHandle.getLocalFragmentInstanceId())) {
        logger.info("Resources of aborted sink handle {} has already been released", sinkHandle);
      }
      sinkHandles.remove(sinkHandle.getLocalFragmentInstanceId());
    }

    @Override
    public void onFailure(ISinkHandle sinkHandle, Throwable t) {
      logger.error("Sink handle {} failed due to {}", sinkHandle, t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  private final LocalMemoryManager localMemoryManager;
  private final Supplier<TsBlockSerde> tsBlockSerdeFactory;
  private final ExecutorService executorService;
  private final IClientManager<TEndPoint, SyncDataNodeDataBlockServiceClient>
      dataBlockServiceClientManager;
  private final Map<TFragmentInstanceId, Map<String, ISourceHandle>> sourceHandles;
  private final Map<TFragmentInstanceId, ISinkHandle> sinkHandles;

  private DataBlockServiceImpl dataBlockService;

  public DataBlockManager(
      LocalMemoryManager localMemoryManager,
      Supplier<TsBlockSerde> tsBlockSerdeFactory,
      ExecutorService executorService,
      IClientManager<TEndPoint, SyncDataNodeDataBlockServiceClient> dataBlockServiceClientManager) {
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.tsBlockSerdeFactory = Validate.notNull(tsBlockSerdeFactory);
    this.executorService = Validate.notNull(executorService);
    this.dataBlockServiceClientManager = Validate.notNull(dataBlockServiceClientManager);
    sourceHandles = new ConcurrentHashMap<>();
    sinkHandles = new ConcurrentHashMap<>();
  }

  public DataBlockServiceImpl getOrCreateDataBlockServiceImpl() {
    if (dataBlockService == null) {
      dataBlockService = new DataBlockServiceImpl();
    }
    return dataBlockService;
  }

  @Override
  public synchronized ISinkHandle createLocalSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      // TODO: replace with callbacks to decouple DataBlockManager from FragmentInstanceContext
      FragmentInstanceContext instanceContext) {
    if (sinkHandles.containsKey(localFragmentInstanceId)) {
      throw new IllegalStateException(
          "Local sink handle for " + localFragmentInstanceId + " exists.");
    }

    logger.debug(
        "Create local sink handle to plan node {} of {} for {}",
        remotePlanNodeId,
        remoteFragmentInstanceId,
        localFragmentInstanceId);

    SharedTsBlockQueue queue;
    if (sourceHandles.containsKey(remoteFragmentInstanceId)
        && sourceHandles.get(remoteFragmentInstanceId).containsKey(remotePlanNodeId)) {
      logger.debug("Get shared tsblock queue from local source handle");
      queue =
          ((LocalSourceHandle) sourceHandles.get(remoteFragmentInstanceId).get(remotePlanNodeId))
              .getSharedTsBlockQueue();
    } else {
      logger.debug("Create shared tsblock queue");
      queue = new SharedTsBlockQueue(remoteFragmentInstanceId, localMemoryManager);
    }

    LocalSinkHandle localSinkHandle =
        new LocalSinkHandle(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            queue,
            new SinkHandleListenerImpl(instanceContext, instanceContext::failed));
    sinkHandles.put(localFragmentInstanceId, localSinkHandle);
    return localSinkHandle;
  }

  @Override
  public ISinkHandle createSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      // TODO: replace with callbacks to decouple DataBlockManager from FragmentInstanceContext
      FragmentInstanceContext instanceContext) {
    if (sinkHandles.containsKey(localFragmentInstanceId)) {
      throw new IllegalStateException("Sink handle for " + localFragmentInstanceId + " exists.");
    }

    logger.debug(
        "Create sink handle to plan node {} of {} for {}",
        remotePlanNodeId,
        remoteFragmentInstanceId,
        localFragmentInstanceId);

    SinkHandle sinkHandle =
        new SinkHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localFragmentInstanceId,
            localMemoryManager,
            executorService,
            tsBlockSerdeFactory.get(),
            new SinkHandleListenerImpl(instanceContext, instanceContext::failed),
            dataBlockServiceClientManager);
    sinkHandles.put(localFragmentInstanceId, sinkHandle);
    return sinkHandle;
  }

  @Override
  public synchronized ISourceHandle createLocalSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      IDataBlockManagerCallback<Throwable> onFailureCallback) {
    if (sourceHandles.containsKey(localFragmentInstanceId)
        && sourceHandles.get(localFragmentInstanceId).containsKey(localPlanNodeId)) {
      throw new IllegalStateException(
          "Source handle for plan node "
              + localPlanNodeId
              + " of "
              + localFragmentInstanceId
              + " exists.");
    }

    logger.debug(
        "Create local source handle from {} for plan node {} of {}",
        remoteFragmentInstanceId,
        localPlanNodeId,
        localFragmentInstanceId);
    SharedTsBlockQueue queue;
    if (sinkHandles.containsKey(remoteFragmentInstanceId)) {
      logger.debug("Get shared tsblock queue from local sink handle");
      queue = ((LocalSinkHandle) sinkHandles.get(remoteFragmentInstanceId)).getSharedTsBlockQueue();
    } else {
      logger.debug("Create shared tsblock queue");
      queue = new SharedTsBlockQueue(localFragmentInstanceId, localMemoryManager);
    }
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            queue,
            new SourceHandleListenerImpl(onFailureCallback));
    sourceHandles
        .computeIfAbsent(localFragmentInstanceId, key -> new ConcurrentHashMap<>())
        .put(localPlanNodeId, localSourceHandle);
    return localSourceHandle;
  }

  @Override
  public ISourceHandle createSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      IDataBlockManagerCallback<Throwable> onFailureCallback) {
    if (sourceHandles.containsKey(localFragmentInstanceId)
        && sourceHandles.get(localFragmentInstanceId).containsKey(localPlanNodeId)) {
      throw new IllegalStateException(
          "Source handle for plan node "
              + localPlanNodeId
              + " of "
              + localFragmentInstanceId
              + " exists.");
    }

    logger.debug(
        "Create source handle from {} for plan node {} of {}",
        remoteFragmentInstanceId,
        localPlanNodeId,
        localFragmentInstanceId);

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            localMemoryManager,
            executorService,
            tsBlockSerdeFactory.get(),
            new SourceHandleListenerImpl(onFailureCallback),
            dataBlockServiceClientManager);
    sourceHandles
        .computeIfAbsent(localFragmentInstanceId, key -> new ConcurrentHashMap<>())
        .put(localPlanNodeId, sourceHandle);
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
      Map<String, ISourceHandle> planNodeIdToSourceHandle = sourceHandles.get(fragmentInstanceId);
      for (Entry<String, ISourceHandle> entry : planNodeIdToSourceHandle.entrySet()) {
        logger.info("Close source handle {}", sourceHandles);
        entry.getValue().abort();
      }
      sourceHandles.remove(fragmentInstanceId);
    }
  }
}
