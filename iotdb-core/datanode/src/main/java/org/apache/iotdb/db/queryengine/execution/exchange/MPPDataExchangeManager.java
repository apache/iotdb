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

package org.apache.iotdb.db.queryengine.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.queryengine.exception.exchange.GetTsBlockFromClosedOrAbortedChannelException;
import org.apache.iotdb.db.queryengine.exception.exchange.SinkChannelClosedOrAbortedTException;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISink;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.PipelineSourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.SourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet;
import org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService;
import org.apache.iotdb.mpp.rpc.thrift.TAcknowledgeDataBlockEvent;
import org.apache.iotdb.mpp.rpc.thrift.TCloseSinkChannelEvent;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.iotdb.db.queryengine.common.DataNodeEndPoints.isSameNode;
import static org.apache.iotdb.db.queryengine.common.FragmentInstanceId.createFullId;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.GET_DATA_BLOCK_TASK_SERVER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCostMetricSet.SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.GET_DATA_BLOCK_NUM_SERVER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.ON_ACKNOWLEDGE_DATA_BLOCK_NUM_SERVER;
import static org.apache.iotdb.db.queryengine.metric.DataExchangeCountMetricSet.SEND_NEW_DATA_BLOCK_NUM_SERVER;

public class MPPDataExchangeManager implements IMPPDataExchangeManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(MPPDataExchangeManager.class);

  // region =========== MPPDataExchangeServiceImpl ===========

  /** Handle thrift communications. */
  class MPPDataExchangeServiceImpl implements MPPDataExchangeService.Iface {
    private final DataExchangeCostMetricSet DATA_EXCHANGE_COST_METRICS =
        DataExchangeCostMetricSet.getInstance();
    private final DataExchangeCountMetricSet DATA_EXCHANGE_COUNT_METRICS =
        DataExchangeCountMetricSet.getInstance();

    @Override
    public TGetDataBlockResponse getDataBlock(TGetDataBlockRequest req) throws TException {
      long startTime = System.nanoTime();
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(
              createFullId(
                  req.sourceFragmentInstanceId.queryId,
                  req.sourceFragmentInstanceId.fragmentId,
                  req.sourceFragmentInstanceId.instanceId))) {
        LOGGER.debug(
            "[ProcessGetTsBlockRequest] sequence ID in [{}, {})",
            req.getStartSequenceId(),
            req.getEndSequenceId());
        TGetDataBlockResponse resp = new TGetDataBlockResponse(new ArrayList<>());
        ISinkHandle sinkHandle = shuffleSinkHandles.get(req.getSourceFragmentInstanceId());
        if (sinkHandle == null) {
          return resp;
        }
        // index of the channel must be a SinkChannel
        SinkChannel sinkChannel = (SinkChannel) (sinkHandle.getChannel(req.getIndex()));
        for (int i = req.getStartSequenceId(); i < req.getEndSequenceId(); i++) {
          try {
            ByteBuffer serializedTsBlock = sinkChannel.getSerializedTsBlock(i);
            resp.addToTsBlocks(serializedTsBlock);
          } catch (GetTsBlockFromClosedOrAbortedChannelException e) {
            throw new SinkChannelClosedOrAbortedTException(e);
          } catch (IllegalStateException | IOException e) {
            throw new TException(e);
          }
        }
        return resp;
      } finally {
        DATA_EXCHANGE_COST_METRICS.recordDataExchangeCost(
            GET_DATA_BLOCK_TASK_SERVER, System.nanoTime() - startTime);
        DATA_EXCHANGE_COUNT_METRICS.recordDataBlockNum(
            GET_DATA_BLOCK_NUM_SERVER, req.getEndSequenceId() - req.getStartSequenceId());
      }
    }

    @Override
    public void onAcknowledgeDataBlockEvent(TAcknowledgeDataBlockEvent e) {
      long startTime = System.nanoTime();
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(
              createFullId(
                  e.sourceFragmentInstanceId.queryId,
                  e.sourceFragmentInstanceId.fragmentId,
                  e.sourceFragmentInstanceId.instanceId))) {
        LOGGER.debug(
            "Received AcknowledgeDataBlockEvent for TsBlocks whose sequence ID are in [{}, {}) from {}.",
            e.getStartSequenceId(),
            e.getEndSequenceId(),
            e.getSourceFragmentInstanceId());
        ISinkHandle sinkHandle = shuffleSinkHandles.get(e.getSourceFragmentInstanceId());
        if (sinkHandle == null) {
          LOGGER.debug(
              "received ACK event but target FragmentInstance[{}] is not found.",
              e.getSourceFragmentInstanceId());
          return;
        }
        // index of the channel must be a SinkChannel
        ((SinkChannel) (sinkHandle.getChannel(e.getIndex())))
            .acknowledgeTsBlock(e.getStartSequenceId(), e.getEndSequenceId());
      } catch (Throwable t) {
        LOGGER.warn(
            "ack TsBlock [{}, {}) failed.", e.getStartSequenceId(), e.getEndSequenceId(), t);
        throw t;
      } finally {
        DATA_EXCHANGE_COST_METRICS.recordDataExchangeCost(
            ON_ACKNOWLEDGE_DATA_BLOCK_EVENT_TASK_SERVER, System.nanoTime() - startTime);
        DATA_EXCHANGE_COUNT_METRICS.recordDataBlockNum(
            ON_ACKNOWLEDGE_DATA_BLOCK_NUM_SERVER, e.getEndSequenceId() - e.getStartSequenceId());
      }
    }

    @Override
    public void onCloseSinkChannelEvent(TCloseSinkChannelEvent e) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(
              createFullId(
                  e.sourceFragmentInstanceId.queryId,
                  e.sourceFragmentInstanceId.fragmentId,
                  e.sourceFragmentInstanceId.instanceId))) {
        LOGGER.debug(
            "Closed source handle of ShuffleSinkHandle {}, channel index: {}.",
            e.getSourceFragmentInstanceId(),
            e.getIndex());
        lockClosedChannelsLock();
        ISinkHandle sinkHandle = shuffleSinkHandles.get(e.getSourceFragmentInstanceId());
        if (sinkHandle == null) {
          LOGGER.debug(
              "received CloseSinkChannelEvent but target FragmentInstance[{}] is not found.",
              e.getSourceFragmentInstanceId());
          markClosedChannel(e.getSourceFragmentInstanceId(), e.getIndex());
          return;
        }
        sinkHandle.getChannel(e.getIndex()).close();
      } catch (Throwable t) {
        LOGGER.warn(
            "Close channel of ShuffleSinkHandle {}, index {} failed.",
            e.getSourceFragmentInstanceId(),
            e.getIndex(),
            t);
        throw t;
      } finally {
        unlockClosedChannelsLock();
      }
    }

    @Override
    public void onNewDataBlockEvent(TNewDataBlockEvent e) throws TException {
      long startTime = System.nanoTime();
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(createFullIdFrom(e.targetFragmentInstanceId, e.targetPlanNodeId))) {
        LOGGER.debug(
            "New data block event received, for plan node {} of {} from {}.",
            e.getTargetPlanNodeId(),
            e.getTargetFragmentInstanceId(),
            e.getSourceFragmentInstanceId());

        Map<String, ISourceHandle> sourceHandleMap =
            sourceHandles.get(e.getTargetFragmentInstanceId());
        SourceHandle sourceHandle =
            sourceHandleMap == null
                ? null
                : (SourceHandle) sourceHandleMap.get(e.getTargetPlanNodeId());

        if (sourceHandle == null || sourceHandle.isAborted() || sourceHandle.isFinished()) {
          // In some scenario, when the SourceHandle sends the data block ACK event, its upstream
          // may
          // have already been stopped. For example, in the read whit LimitOperator, the downstream
          // FragmentInstance may be finished, although the upstream is still working.
          LOGGER.debug(
              "received NewDataBlockEvent but the downstream FragmentInstance[{}] is not found",
              e.getTargetFragmentInstanceId());
          return;
        }

        sourceHandle.updatePendingDataBlockInfo(e.getStartSequenceId(), e.getBlockSizes());
      } finally {
        DATA_EXCHANGE_COST_METRICS.recordDataExchangeCost(
            SEND_NEW_DATA_BLOCK_EVENT_TASK_SERVER, System.nanoTime() - startTime);
        DATA_EXCHANGE_COUNT_METRICS.recordDataBlockNum(
            SEND_NEW_DATA_BLOCK_NUM_SERVER, e.getBlockSizes().size());
      }
    }

    @Override
    public void onEndOfDataBlockEvent(TEndOfDataBlockEvent e) throws TException {
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(createFullIdFrom(e.targetFragmentInstanceId, e.targetPlanNodeId))) {
        LOGGER.debug(
            "End of data block event received, for plan node {} of {} from {}.",
            e.getTargetPlanNodeId(),
            e.getTargetFragmentInstanceId(),
            e.getSourceFragmentInstanceId());

        Map<String, ISourceHandle> sourceHandleMap =
            sourceHandles.get(e.getTargetFragmentInstanceId());
        SourceHandle sourceHandle =
            sourceHandleMap == null
                ? null
                : (SourceHandle) sourceHandleMap.get(e.getTargetPlanNodeId());

        if (sourceHandle == null || sourceHandle.isAborted() || sourceHandle.isFinished()) {
          LOGGER.debug(
              "received onEndOfDataBlockEvent but the downstream FragmentInstance[{}] is not found",
              e.getTargetFragmentInstanceId());
          return;
        }

        sourceHandle.setNoMoreTsBlocks(e.getLastSequenceId());
      }
    }
  }

  // endregion

  // region =========== listener ===========

  public interface SourceHandleListener {
    void onFinished(ISourceHandle sourceHandle);

    void onAborted(ISourceHandle sourceHandle);

    void onFailure(ISourceHandle sourceHandle, Throwable t);
  }

  public interface SinkListener {
    void onFinish(ISink sink);

    void onEndOfBlocks(ISink sink);

    Optional<Throwable> onAborted(ISink sink);

    void onFailure(ISink sink, Throwable t);
  }

  /** Listen to the state changes of a source handle. */
  class SourceHandleListenerImpl implements SourceHandleListener {

    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public SourceHandleListenerImpl(IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinished(ISourceHandle sourceHandle) {
      LOGGER.debug("[ScHListenerOnFinish]");
      Map<String, ISourceHandle> sourceHandleMap =
          sourceHandles.get(sourceHandle.getLocalFragmentInstanceId());
      if (sourceHandleMap == null
          || sourceHandleMap.remove(sourceHandle.getLocalPlanNodeId()) == null) {
        LOGGER.debug("[ScHListenerAlreadyReleased]");
      }

      if (sourceHandleMap != null && sourceHandleMap.isEmpty()) {
        sourceHandles.remove(sourceHandle.getLocalFragmentInstanceId());
      }
    }

    @Override
    public void onAborted(ISourceHandle sourceHandle) {
      LOGGER.debug("[ScHListenerOnAbort]");
      onFinished(sourceHandle);
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      LOGGER.warn("Source handle failed due to: ", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  /**
   * Listen to the state changes of a source handle of pipeline. Since we register nothing in the
   * exchangeManager, so we don't need to remove it too.
   */
  static class PipelineSourceHandleListenerImpl implements SourceHandleListener {

    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public PipelineSourceHandleListenerImpl(
        IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinished(ISourceHandle sourceHandle) {
      LOGGER.debug("[ScHListenerOnFinish]");
    }

    @Override
    public void onAborted(ISourceHandle sourceHandle) {
      LOGGER.debug("[ScHListenerOnAbort]");
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      LOGGER.warn("Source handle failed due to: ", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  /** Listen to the state changes of a sink handle. */
  class ShuffleSinkListenerImpl implements SinkListener {

    private final FragmentInstanceContext context;
    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public ShuffleSinkListenerImpl(
        FragmentInstanceContext context,
        IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.context = context;
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinish(ISink sink) {
      LOGGER.debug("[ShuffleSinkHandleListenerOnFinish]");
      shuffleSinkHandles.remove(sink.getLocalFragmentInstanceId());
      context.finished();
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      LOGGER.debug("[ShuffleSinkHandleListenerOnEndOfTsBlocks]");
      context.transitionToFlushing();
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      LOGGER.debug("[ShuffleSinkHandleListenerOnAbort]");
      shuffleSinkHandles.remove(sink.getLocalFragmentInstanceId());
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      // TODO: (xingtanzjr) should we remove the sink from MPPDataExchangeManager ?
      LOGGER.warn("Sink failed due to", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  class ISinkChannelListenerImpl implements SinkListener {

    private final TFragmentInstanceId shuffleSinkHandleId;

    private final FragmentInstanceContext context;
    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    private final AtomicInteger cnt;

    private final AtomicBoolean hasDecremented = new AtomicBoolean(false);

    public ISinkChannelListenerImpl(
        TFragmentInstanceId localFragmentInstanceId,
        FragmentInstanceContext context,
        IMPPDataExchangeManagerCallback<Throwable> onFailureCallback,
        AtomicInteger cnt) {
      this.shuffleSinkHandleId = localFragmentInstanceId;
      this.context = context;
      this.onFailureCallback = onFailureCallback;
      this.cnt = cnt;
    }

    @Override
    public void onFinish(ISink sink) {
      LOGGER.debug("[SkHListenerOnFinish]");
      decrementCnt();
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      LOGGER.debug("[SkHListenerOnEndOfTsBlocks]");
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      LOGGER.debug("[SkHListenerOnAbort]");
      decrementCnt();
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      LOGGER.warn("ISinkChannel failed due to", t);
      decrementCnt();
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }

    private void decrementCnt() {
      if (hasDecremented.compareAndSet(false, true) && (cnt.decrementAndGet() == 0)) {
        closeShuffleSinkHandle();
      }
    }

    private void closeShuffleSinkHandle() {
      ISinkHandle sinkHandle = shuffleSinkHandles.remove(shuffleSinkHandleId);
      if (sinkHandle != null) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Close ShuffleSinkHandle: {}", shuffleSinkHandleId);
        }
        sinkHandle.close();
      }
    }
  }

  /**
   * Listen to the state changes of a sink handle of pipeline. And since the finish of pipeline sink
   * handle doesn't equal the finish of the whole fragment, therefore we don't need to notify
   * fragment context. But if it's aborted or failed, it can lead to the total fail.
   */
  static class PipelineSinkListenerImpl implements SinkListener {

    private final FragmentInstanceContext context;
    private final IMPPDataExchangeManagerCallback<Throwable> onFailureCallback;

    public PipelineSinkListenerImpl(
        FragmentInstanceContext context,
        IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
      this.context = context;
      this.onFailureCallback = onFailureCallback;
    }

    @Override
    public void onFinish(ISink sink) {
      LOGGER.debug("[SkHListenerOnFinish]");
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      LOGGER.debug("[SkHListenerOnEndOfTsBlocks]");
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      LOGGER.debug("[SkHListenerOnAbort]");
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      LOGGER.warn("Sink handle failed due to", t);
      if (onFailureCallback != null) {
        onFailureCallback.call(t);
      }
    }
  }

  // endregion

  // region =========== MPPDataExchangeManager ===========

  private final LocalMemoryManager localMemoryManager;
  private final Supplier<TsBlockSerde> tsBlockSerdeFactory;
  private final ExecutorService executorService;
  private final IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
      mppDataExchangeServiceClientManager;
  private final Map<TFragmentInstanceId, Map<String, ISourceHandle>> sourceHandles;

  /** Each FI has only one ShuffleSinkHandle. */
  private final Map<TFragmentInstanceId, ISinkHandle> shuffleSinkHandles;

  private final Map<TFragmentInstanceId, Set<Integer>> closedChannels;

  private final Lock closedChannelsLock;

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
    shuffleSinkHandles = new ConcurrentHashMap<>();
    closedChannelsLock = new ReentrantLock();
    closedChannels = new ConcurrentHashMap<>();
    ;
  }

  public MPPDataExchangeServiceImpl getOrCreateMPPDataExchangeServiceImpl() {
    if (mppDataExchangeService == null) {
      mppDataExchangeService = new MPPDataExchangeServiceImpl();
    }
    return mppDataExchangeService;
  }

  public void deRegisterFragmentInstanceFromMemoryPool(String queryId, String fragmentInstanceId) {
    localMemoryManager
        .getQueryPool()
        .deRegisterFragmentInstanceToQueryMemoryMap(queryId, fragmentInstanceId);
  }

  public void lockClosedChannelsLock() {
    closedChannelsLock.lock();
  }

  public void unlockClosedChannelsLock() {
    closedChannelsLock.unlock();
  }

  public void markClosedChannel(TFragmentInstanceId sourceId, int index) {
    closedChannels
        .computeIfAbsent(sourceId, key -> Collections.synchronizedSet(new HashSet<>()))
        .add(index);
  }

  /** Avoid memory leak. */
  public void clearClosedChannels(TFragmentInstanceId sourceId) {
    closedChannels.remove(sourceId);
  }

  public LocalMemoryManager getLocalMemoryManager() {
    return localMemoryManager;
  }

  public int getShuffleSinkHandleSize() {
    return shuffleSinkHandles.size();
  }

  public int getSourceHandleSize() {
    // risk of Integer overflow
    return sourceHandles.values().stream().mapToInt(Map::size).sum();
  }

  private synchronized ISinkChannel createLocalSinkChannel(
      TFragmentInstanceId localFragmentInstanceId,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      String localPlanNodeId,
      // TODO: replace with callbacks to decouple MPPDataExchangeManager from
      // FragmentInstanceContext
      FragmentInstanceContext instanceContext,
      AtomicInteger cnt,
      int index) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Create local sink handle to plan node {} of {} for {}",
          remotePlanNodeId,
          remoteFragmentInstanceId,
          localFragmentInstanceId);
    }

    SharedTsBlockQueue queue;
    Map<String, ISourceHandle> sourceHandleMap = sourceHandles.get(remoteFragmentInstanceId);
    LocalSourceHandle localSourceHandle =
        sourceHandleMap == null ? null : (LocalSourceHandle) sourceHandleMap.get(remotePlanNodeId);
    if (localSourceHandle != null) {
      LOGGER.debug("Get SharedTsBlockQueue from local source handle");
      queue = localSourceHandle.getSharedTsBlockQueue();
    } else {
      LOGGER.debug("Create SharedTsBlockQueue");
      queue =
          new SharedTsBlockQueue(
              localFragmentInstanceId, localPlanNodeId, localMemoryManager, executorService);
      queue.setIndexOfChannel(index);
    }
    try {
      // try to lock closedChannelsLock to guarantee that either LocalSinkChannel of this
      // SharedTsBlockQueue is set,
      // or the corresponding SharedTsBlockQueue of LocalSourceHandle marks the closed
      // LocalSinkChannel properly when closing.
      lockClosedChannelsLock();
      return new LocalSinkChannel(
          localFragmentInstanceId,
          queue,
          new ISinkChannelListenerImpl(
              localFragmentInstanceId, instanceContext, instanceContext::failed, cnt));
    } finally {
      unlockClosedChannelsLock();
    }
  }

  /**
   * As we know the upstream and downstream node of shared queue, we don't need to put it into the
   * sink map.
   */
  public ISinkChannel createLocalSinkChannelForPipeline(
      DriverContext driverContext, String planNodeId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Create local sink handle for {}", driverContext.getDriverTaskID());
    }
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            driverContext.getDriverTaskID().getFragmentInstanceId().toThrift(),
            planNodeId,
            localMemoryManager,
            executorService);
    queue.allowAddingTsBlock();
    return new LocalSinkChannel(
        queue,
        new PipelineSinkListenerImpl(
            driverContext.getFragmentInstanceContext(), driverContext::failed));
  }

  private ISinkChannel createSinkChannel(
      TFragmentInstanceId localFragmentInstanceId,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      String remotePlanNodeId,
      String localPlanNodeId,
      // TODO: replace with callbacks to decouple MPPDataExchangeManager from
      // FragmentInstanceContext
      FragmentInstanceContext instanceContext,
      AtomicInteger cnt) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Create sink handle to plan node {} of {} for {}",
          remotePlanNodeId,
          remoteFragmentInstanceId,
          localFragmentInstanceId);
    }

    return new SinkChannel(
        remoteEndpoint,
        remoteFragmentInstanceId,
        remotePlanNodeId,
        localPlanNodeId,
        localFragmentInstanceId,
        localMemoryManager,
        executorService,
        tsBlockSerdeFactory.get(),
        new ISinkChannelListenerImpl(
            localFragmentInstanceId, instanceContext, instanceContext::failed, cnt),
        mppDataExchangeServiceClientManager);
  }

  @Override
  public ISinkHandle createShuffleSinkHandle(
      List<DownStreamChannelLocation> downStreamChannelLocationList,
      DownStreamChannelIndex downStreamChannelIndex,
      ShuffleSinkHandle.ShuffleStrategyEnum shuffleStrategyEnum,
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      // TODO: replace with callbacks to decouple MPPDataExchangeManager from
      // FragmentInstanceContext
      FragmentInstanceContext instanceContext) {
    if (shuffleSinkHandles.containsKey(localFragmentInstanceId)) {
      throw new IllegalStateException(
          "ShuffleSinkHandle for " + localFragmentInstanceId + " is in the map.");
    }

    int channelNum = downStreamChannelLocationList.size();
    AtomicInteger cnt = new AtomicInteger(channelNum);
    List<ISinkChannel> downStreamChannelList =
        IntStream.range(0, channelNum)
            .mapToObj(
                index ->
                    createChannelForShuffleSink(
                        localFragmentInstanceId,
                        localPlanNodeId,
                        downStreamChannelLocationList.get(index),
                        instanceContext,
                        cnt,
                        index))
            .collect(Collectors.toList());

    ShuffleSinkHandle shuffleSinkHandle =
        new ShuffleSinkHandle(
            localFragmentInstanceId,
            downStreamChannelList,
            downStreamChannelIndex,
            shuffleStrategyEnum,
            new ShuffleSinkListenerImpl(instanceContext, instanceContext::failed));
    try {
      // try to lock closedChannelsLock to guarantee that either shuffleSinkHandles can be found,
      // or the onClosedSinkChannelEvent marks the closed SinkChannel properly.
      lockClosedChannelsLock();
      shuffleSinkHandles.put(localFragmentInstanceId, shuffleSinkHandle);
    } finally {
      unlockClosedChannelsLock();
    }

    Set<Integer> closed = closedChannels.remove(localFragmentInstanceId);
    if (closed != null) {
      closed.forEach(index -> downStreamChannelList.get(index).close());
    }

    return shuffleSinkHandle;
  }

  private ISinkChannel createChannelForShuffleSink(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      DownStreamChannelLocation downStreamChannelLocation,
      FragmentInstanceContext instanceContext,
      AtomicInteger cnt,
      int index) {
    if (isSameNode(downStreamChannelLocation.getRemoteEndpoint())) {
      return createLocalSinkChannel(
          localFragmentInstanceId,
          downStreamChannelLocation.getRemoteFragmentInstanceId(),
          downStreamChannelLocation.getRemotePlanNodeId(),
          localPlanNodeId,
          instanceContext,
          cnt,
          index);
    } else {
      return createSinkChannel(
          localFragmentInstanceId,
          downStreamChannelLocation.getRemoteEndpoint(),
          downStreamChannelLocation.getRemoteFragmentInstanceId(),
          downStreamChannelLocation.getRemotePlanNodeId(),
          localPlanNodeId,
          instanceContext,
          cnt);
    }
  }

  /**
   * As we know the upstream and downstream node of shared queue, we don't need to put it into the
   * sourceHandle map.
   */
  public ISourceHandle createLocalSourceHandleForPipeline(
      SharedTsBlockQueue queue, DriverContext context) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Create local source handle for {}", context.getDriverTaskID());
    }
    return new PipelineSourceHandle(
        queue,
        new PipelineSourceHandleListenerImpl(context::failed),
        context.getDriverTaskID().toString());
  }

  public synchronized ISourceHandle createLocalSourceHandleForFragment(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      String remotePlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      int index,
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

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Create local source handle from {} for plan node {} of {}",
          remoteFragmentInstanceId,
          localPlanNodeId,
          localFragmentInstanceId);
    }

    SharedTsBlockQueue queue;
    ISinkHandle sinkHandle = shuffleSinkHandles.get(remoteFragmentInstanceId);
    if (sinkHandle != null) {
      LOGGER.debug("Get SharedTsBlockQueue from local sink handle");
      queue = ((LocalSinkChannel) (sinkHandle.getChannel(index))).getSharedTsBlockQueue();
    } else {
      LOGGER.debug("Create SharedTsBlockQueue");
      queue =
          new SharedTsBlockQueue(
              remoteFragmentInstanceId, remotePlanNodeId, localMemoryManager, executorService);
      queue.setIndexOfChannel(index);
    }
    LocalSourceHandle localSourceHandle =
        new LocalSourceHandle(
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
      int indexOfUpstreamSinkHandle,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
    Map<String, ISourceHandle> sourceHandleMap = sourceHandles.get(localFragmentInstanceId);
    if (sourceHandleMap != null && sourceHandleMap.containsKey(localPlanNodeId)) {
      throw new IllegalStateException(
          "Source handle for plan node "
              + localPlanNodeId
              + " of "
              + localFragmentInstanceId
              + " exists.");
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Create source handle from {} for plan node {} of {}",
          remoteFragmentInstanceId,
          localPlanNodeId,
          localFragmentInstanceId);
    }

    SourceHandle sourceHandle =
        new SourceHandle(
            remoteEndpoint,
            remoteFragmentInstanceId,
            localFragmentInstanceId,
            localPlanNodeId,
            indexOfUpstreamSinkHandle,
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
    LOGGER.debug("[StartForceReleaseFIDataExchangeResource]");
    ISink sinkHandle = shuffleSinkHandles.get(fragmentInstanceId);
    if (sinkHandle != null) {
      sinkHandle.abort();
      shuffleSinkHandles.remove(fragmentInstanceId);
    }
    Map<String, ISourceHandle> planNodeIdToSourceHandle = sourceHandles.get(fragmentInstanceId);
    if (planNodeIdToSourceHandle != null) {
      for (Entry<String, ISourceHandle> entry : planNodeIdToSourceHandle.entrySet()) {
        LOGGER.debug("[CloseSourceHandle] {}", entry.getKey());
        entry.getValue().abort();
      }
      sourceHandles.remove(fragmentInstanceId);
    }
    LOGGER.debug("[EndForceReleaseFIDataExchangeResource]");
  }

  /**
   * Create FullId with suffix.
   *
   * @param suffix should be like [PlanNodeId].SourceHandle/SinHandle
   */
  public static String createFullIdFrom(TFragmentInstanceId fragmentInstanceId, String suffix) {
    return createFullId(
            fragmentInstanceId.queryId,
            fragmentInstanceId.fragmentId,
            fragmentInstanceId.instanceId)
        + "."
        + suffix;
  }
  // endregion
}
