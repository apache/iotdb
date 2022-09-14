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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TEndOfDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockRequest;
import org.apache.iotdb.mpp.rpc.thrift.TGetDataBlockResponse;
import org.apache.iotdb.mpp.rpc.thrift.TNewDataBlockEvent;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import io.airlift.concurrent.SetThreadName;
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

import static org.apache.iotdb.db.mpp.common.FragmentInstanceId.createFullId;

public class MPPDataExchangeManager implements IMPPDataExchangeManager {

  private static final Logger logger = LoggerFactory.getLogger(MPPDataExchangeManager.class);

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
  class MPPDataExchangeServiceImpl implements MPPDataExchangeService.Iface {

    @Override
    public TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(createFullIdFrom(req.sourceFragmentInstanceId, "SinkHandle"))) {
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
    }

    @Override
    public void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(createFullIdFrom(e.sourceFragmentInstanceId, "SinkHandle"))) {
        logger.debug(
            "Acknowledge data block event received, for data blocks whose sequence ID in [{}, {}) from {}.",
            e.getStartSequenceId(),
            e.getEndSequenceId(),
            e.getSourceFragmentInstanceId());
        if (!sinkHandles.containsKey(e.getSourceFragmentInstanceId())) {
          logger.warn(
              "received ACK event but target FragmentInstance[{}] is not found.",
              e.getSourceFragmentInstanceId());
          return;
        }
        ((SinkHandle) sinkHandles.get(e.getSourceFragmentInstanceId()))
            .acknowledgeTsBlock(e.getStartSequenceId(), e.getEndSequenceId());
      } catch (Throwable t) {
        logger.error(
            "ack TsBlock [{}, {}) failed.", e.getStartSequenceId(), e.getEndSequenceId(), t);
        throw t;
      }
    }

    @Override
    public void onNewDataBlockEvent(TNewDataBlockEvent e) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(
              createFullIdFrom(e.targetFragmentInstanceId, e.targetPlanNodeId + ".SourceHandle"))) {
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
                .isAborted()
            || sourceHandles
                .get(e.getTargetFragmentInstanceId())
                .get(e.getTargetPlanNodeId())
                .isFinished()) {
          // In some scenario, when the SourceHandle sends the data block ACK event, its upstream
          // may
          // have already been stopped. For example, in the query whit LimitOperator, the downstream
          // FragmentInstance may be finished, although the upstream is still working.
          logger.warn(
              "received NewDataBlockEvent but the downstream FragmentInstance[{}] is not found",
              e.getTargetFragmentInstanceId());
          return;
        }

        SourceHandle sourceHandle =
            (SourceHandle)
                sourceHandles.get(e.getTargetFragmentInstanceId()).get(e.getTargetPlanNodeId());
        sourceHandle.updatePendingDataBlockInfo(e.getStartSequenceId(), e.getBlockSizes());
      }
    }

    @Override
    public void onEndOfDataBlockEvent(TEndOfDataBlockEvent e) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(
              createFullIdFrom(e.targetFragmentInstanceId, e.targetPlanNodeId + ".SourceHandle"))) {
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
                .isAborted()
            || sourceHandles
                .get(e.getTargetFragmentInstanceId())
                .get(e.getTargetPlanNodeId())
                .isFinished()) {
          logger.warn(
              "received onEndOfDataBlockEvent but the downstream FragmentInstance[{}] is not found",
              e.getTargetFragmentInstanceId());
          return;
        }
        SourceHandle sourceHandle =
            (SourceHandle)
                sourceHandles
                    .getOrDefault(e.getTargetFragmentInstanceId(), Collections.emptyMap())
                    .get(e.getTargetPlanNodeId());
        sourceHandle.setNoMoreTsBlocks(e.getLastSequenceId());
      }
    }
  }

  /** Listen to the state changes of a source handle. */
  class SourceHandleListenerImpl implements SourceHandleListener {

    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public SourceHandleListenerImpl(IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinished(ISourceHandle sourceHandle) {
      logger.info("finished and release resources");
      if (!sourceHandles.containsKey(sourceHandle.getLocalFragmentInstanceId())
          || !sourceHandles
              .get(sourceHandle.getLocalFragmentInstanceId())
              .containsKey(sourceHandle.getLocalPlanNodeId())) {
        logger.info("resources has already been released");
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
      logger.info("onAborted is invoked");
      onFinished(sourceHandle);
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      logger.error("Source handle failed due to: ", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  /** Listen to the state changes of a sink handle. */
  class SinkHandleListenerImpl implements SinkHandleListener {

    private final FragmentInstanceContext context;
    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public SinkHandleListenerImpl(
        FragmentInstanceContext context,
        IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.context = context;
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinish(ISinkHandle sinkHandle) {
      logger.info("onFinish is invoked");
      removeFromMPPDataExchangeManager(sinkHandle);
      context.finished();
    }

    @Override
    public void onEndOfBlocks(ISinkHandle sinkHandle) {
      context.transitionToFlushing();
    }

    @Override
    public void onAborted(ISinkHandle sinkHandle) {
      logger.info("onAborted is invoked");
      removeFromMPPDataExchangeManager(sinkHandle);
    }

    private void removeFromMPPDataExchangeManager(ISinkHandle sinkHandle) {
      logger.info("release resources of finished sink handle");
      if (!sinkHandles.containsKey(sinkHandle.getLocalFragmentInstanceId())) {
        logger.info("resources already been released");
      }
      sinkHandles.remove(sinkHandle.getLocalFragmentInstanceId());
    }

    @Override
    public void onFailure(ISinkHandle sinkHandle, Throwable t) {
      // TODO: (xingtanzjr) should we remove the sinkHandle from MPPDataExchangeManager ?
      logger.error("Sink handle failed due to", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  private final LocalMemoryManager localMemoryManager;
  private final Supplier<TsBlockSerde> tsBlockSerdeFactory;
  private final ExecutorService executorService;
  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;
  private final Map<TFragmentInstanceId, Map<String, ISourceHandle>> sourceHandles;
  private final Map<TFragmentInstanceId, ISinkHandle> sinkHandles;

  private MPPDataExchangeServiceImpl mppDataExchangeService;

  public MPPDataExchangeManager(
      LocalMemoryManager localMemoryManager,
      Supplier<TsBlockSerde> tsBlockSerdeFactory,
      ExecutorService executorService,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.localMemoryManager = Validate.notNull(localMemoryManager);
    this.tsBlockSerdeFactory = Validate.notNull(tsBlockSerdeFactory);
    this.executorService = Validate.notNull(executorService);
    this.mppDataExchangeServiceClientManager =
        Validate.notNull(mppDataExchangeServiceClientManager);
    sourceHandles = new ConcurrentHashMap<>();
    sinkHandles = new ConcurrentHashMap<>();
  }

  public MPPDataExchangeServiceImpl getOrCreateMPPDataExchangeServiceImpl() {
    if (mppDataExchangeService == null) {
      mppDataExchangeService = new MPPDataExchangeServiceImpl();
    }
    return mppDataExchangeService;
  }

  @Override
  public synchronized ISinkHandle createLocalSinkHandle(
      TFragmentInstanceId localFragmentInstanceId,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      // TODO: replace with callbacks to decouple MPPDataExchangeManager from
      // FragmentInstanceContext
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
      // TODO: replace with callbacks to decouple MPPDataExchangeManager from
      // FragmentInstanceContext
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
            mppDataExchangeServiceClientManager);
    sinkHandles.put(localFragmentInstanceId, sinkHandle);
    return sinkHandle;
  }

  @Override
  public synchronized ISourceHandle createLocalSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
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
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
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
            mppDataExchangeServiceClientManager);
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
    logger.info("Force deregister fragment instance");
    if (sinkHandles.containsKey(fragmentInstanceId)) {
      ISinkHandle sinkHandle = sinkHandles.get(fragmentInstanceId);
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

  /** @param suffix should be like [PlanNodeId].SourceHandle/SinHandle */
  public static String createFullIdFrom(TFragmentInstanceId fragmentInstanceId, String suffix) {
    return createFullId(
            fragmentInstanceId.queryId,
            fragmentInstanceId.fragmentId,
            fragmentInstanceId.instanceId)
        + "."
        + suffix;
  }
}
