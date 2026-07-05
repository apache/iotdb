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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.exception.exchange.GetTsBlockFromClosedOrAbortedChannelException;
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
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.external.commons.lang3.Validate;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.PROCESSGETTSBLOCKREQUEST_SEQUENCE_ID_IN_ARG_ARG,
              req.getStartSequenceId(),
              req.getEndSequenceId());
        }
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
            // Return an empty block list to indicate that getting data block failed this time.
            // The SourceHandle will deal with this signal depending on its state.
            return new TGetDataBlockResponse(new ArrayList<>());
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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages
                  .RECEIVED_ACKNOWLEDGEDATABLOCKEVENT_FOR_TSBLOCKS_WHOSE_SEQUENCE_ID_ARE_IN_ARG_ARG_FROM,
              e.getStartSequenceId(),
              e.getEndSequenceId(),
              e.getSourceFragmentInstanceId());
        }
        ISinkHandle sinkHandle = shuffleSinkHandles.get(e.getSourceFragmentInstanceId());
        if (sinkHandle == null) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                DataNodeQueryMessages
                    .RECEIVED_ACK_EVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND,
                e.getSourceFragmentInstanceId());
          }
          return;
        }
        // index of the channel must be a SinkChannel
        ((SinkChannel) (sinkHandle.getChannel(e.getIndex())))
            .acknowledgeTsBlock(e.getStartSequenceId(), e.getEndSequenceId());
      } catch (Throwable t) {
        LOGGER.warn(
            DataNodeQueryMessages.ACK_TSBLOCK_FAILED,
            e.getStartSequenceId(),
            e.getEndSequenceId(),
            t);
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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.CLOSED_SOURCE_HANDLE_OF_SHUFFLESINKHANDLE_ARG_CHANNEL_INDEX_ARG,
              e.getSourceFragmentInstanceId(),
              e.getIndex());
        }

        ISinkHandle sinkHandle = shuffleSinkHandles.get(e.getSourceFragmentInstanceId());
        if (sinkHandle == null) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                DataNodeQueryMessages
                    .RECEIVED_CLOSESINKCHANNELEVENT_BUT_TARGET_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND,
                e.getSourceFragmentInstanceId());
          }
          return;
        }
        sinkHandle.getChannel(e.getIndex()).close();
      } catch (Throwable t) {
        LOGGER.warn(
            DataNodeQueryMessages.CLOSE_CHANNEL_OF_SHUFFLESINKHANDLE_FAILED,
            e.getSourceFragmentInstanceId(),
            e.getIndex(),
            t);
        throw t;
      }
    }

    @Override
    public void onNewDataBlockEvent(TNewDataBlockEvent e) throws TException {
      long startTime = System.nanoTime();
      try (SetThreadName fragmentInstanceName =
          new SetThreadName(createFullIdFrom(e.targetFragmentInstanceId, e.targetPlanNodeId))) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages.NEW_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG,
              e.getTargetPlanNodeId(),
              e.getTargetFragmentInstanceId(),
              e.getSourceFragmentInstanceId());
        }

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
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                DataNodeQueryMessages
                    .RECEIVED_NEWDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND,
                e.getTargetFragmentInstanceId());
          }
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
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(
              DataNodeQueryMessages
                  .END_OF_DATA_BLOCK_EVENT_RECEIVED_FOR_PLAN_NODE_ARG_OF_ARG_FROM_ARG,
              e.getTargetPlanNodeId(),
              e.getTargetFragmentInstanceId(),
              e.getSourceFragmentInstanceId());
        }

        Map<String, ISourceHandle> sourceHandleMap =
            sourceHandles.get(e.getTargetFragmentInstanceId());
        SourceHandle sourceHandle =
            sourceHandleMap == null
                ? null
                : (SourceHandle) sourceHandleMap.get(e.getTargetPlanNodeId());

        if (sourceHandle == null || sourceHandle.isAborted() || sourceHandle.isFinished()) {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                DataNodeQueryMessages
                    .RECEIVED_ONENDOFDATABLOCKEVENT_BUT_THE_DOWNSTREAM_FRAGMENTINSTANCE_ARG_IS_NOT_FOUND,
                e.getTargetFragmentInstanceId());
          }
          return;
        }

        sourceHandle.setNoMoreTsBlocks(e.getLastSequenceId());
      }
    }

    @Override
    public TSStatus testConnectionEmptyRPC() throws TException {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SCH_LISTENER_ON_FINISH);
      }
      Map<String, ISourceHandle> sourceHandleMap =
          sourceHandles.get(sourceHandle.getLocalFragmentInstanceId());
      if ((sourceHandleMap == null
              || sourceHandleMap.remove(sourceHandle.getLocalPlanNodeId()) == null)
          && LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SCH_LISTENER_ALREADY_RELEASED);
      }

      if (sourceHandleMap != null && sourceHandleMap.isEmpty()) {
        sourceHandles.remove(sourceHandle.getLocalFragmentInstanceId());
      }
    }

    @Override
    public void onAborted(ISourceHandle sourceHandle) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SCH_LISTENER_ON_ABORT);
      }
      onFinished(sourceHandle);
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      LOGGER.warn(DataNodeQueryMessages.SOURCE_HANDLE_FAILED_DUE_TO, t);
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SCH_LISTENER_ON_FINISH);
      }
    }

    @Override
    public void onAborted(ISourceHandle sourceHandle) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SCH_LISTENER_ON_ABORT);
      }
    }

    @Override
    public void onFailure(ISourceHandle sourceHandle, Throwable t) {
      LOGGER.warn(DataNodeQueryMessages.SOURCE_HANDLE_FAILED_DUE_TO, t);
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SHUFFLE_SINK_HANDLE_LISTENER_ON_FINISH);
      }
      shuffleSinkHandles.remove(sink.getLocalFragmentInstanceId());
      context.finished();
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SHUFFLE_SINK_HANDLE_LISTENER_ON_END_OF_TSBLOCKS);
      }
      context.transitionToFlushing();
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SHUFFLE_SINK_HANDLE_LISTENER_ON_ABORT);
      }
      shuffleSinkHandles.remove(sink.getLocalFragmentInstanceId());
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      // TODO: (xingtanzjr) should we remove the sink from MPPDataExchangeManager ?
      LOGGER.warn(DataNodeQueryMessages.SINK_FAILED_DUE_TO, t);
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_FINISH);
      }
      decrementCnt();
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_END_OF_TSBLOCKS);
      }
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_ABORT);
      }
      decrementCnt();
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      LOGGER.warn(DataNodeQueryMessages.ISINKCHANNEL_FAILED_DUE_TO, t);
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
          LOGGER.debug(DataNodeQueryMessages.CLOSE_SHUFFLE_SINK_HANDLE, shuffleSinkHandleId);
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
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_FINISH);
      }
    }

    @Override
    public void onEndOfBlocks(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_END_OF_TSBLOCKS);
      }
    }

    @Override
    public Optional<Throwable> onAborted(ISink sink) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.SKH_LISTENER_ON_ABORT);
      }
      return context.getFailureCause();
    }

    @Override
    public void onFailure(ISink sink, Throwable t) {
      LOGGER.warn(DataNodeQueryMessages.SINK_HANDLE_FAILED_DUE_TO, t);
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

  private MPPDataExchangeServiceImpl mppDataExchangeService;

  public MPPDataExchangeManager(
      LocalMemoryManager localMemoryManager,
      Supplier<TsBlockSerde> tsBlockSerdeFactory,
      ExecutorService executorService,
      IClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
          mppDataExchangeServiceClientManager) {
    this.localMemoryManager =
        Validate.notNull(
            localMemoryManager,
            DataNodeQueryMessages.EXCEPTION_LOCALMEMORYMANAGER_IS_NULL_DOT_69FE497A);
    this.tsBlockSerdeFactory =
        Validate.notNull(
            tsBlockSerdeFactory,
            DataNodeQueryMessages.EXCEPTION_TSBLOCKSERDEFACTORY_IS_NULL_DOT_32EB5BD2);
    this.executorService =
        Validate.notNull(
            executorService, DataNodeQueryMessages.EXCEPTION_EXECUTORSERVICE_IS_NULL_DOT_7B057909);
    this.mppDataExchangeServiceClientManager =
        Validate.notNull(
            mppDataExchangeServiceClientManager,
            DataNodeQueryMessages
                .EXCEPTION_MPPDATAEXCHANGESERVICECLIENTMANAGER_IS_NULL_DOT_F31E746C);
    sourceHandles = new ConcurrentHashMap<>();
    shuffleSinkHandles = new ConcurrentHashMap<>();
  }

  public MPPDataExchangeServiceImpl getOrCreateMPPDataExchangeServiceImpl() {
    if (mppDataExchangeService == null) {
      mppDataExchangeService = new MPPDataExchangeServiceImpl();
    }
    return mppDataExchangeService;
  }

  public void deRegisterFragmentInstanceFromMemoryPool(
      String queryId, String fragmentInstanceId, boolean forceDeregister) {
    localMemoryManager
        .getQueryPool()
        .deRegisterFragmentInstanceFromQueryMemoryMap(queryId, fragmentInstanceId, forceDeregister);
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
      AtomicInteger cnt) {

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodeQueryMessages.CREATE_LOCAL_SINK_HANDLE_TO_PLAN_NODE,
          remotePlanNodeId,
          remoteFragmentInstanceId,
          localFragmentInstanceId);
    }

    SharedTsBlockQueue queue;
    Map<String, ISourceHandle> sourceHandleMap = sourceHandles.get(remoteFragmentInstanceId);
    LocalSourceHandle localSourceHandle =
        sourceHandleMap == null ? null : (LocalSourceHandle) sourceHandleMap.get(remotePlanNodeId);
    if (localSourceHandle != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SOURCE_HANDLE);
      }
      queue = localSourceHandle.getSharedTsBlockQueue();
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.CREATE_SHARED_TSBLOCK_QUEUE);
      }
      queue =
          new SharedTsBlockQueue(
              localFragmentInstanceId,
              localPlanNodeId,
              localMemoryManager,
              executorService,
              instanceContext.isHighestPriority());
    }

    return new LocalSinkChannel(
        localFragmentInstanceId,
        queue,
        new ISinkChannelListenerImpl(
            localFragmentInstanceId, instanceContext, instanceContext::failed, cnt));
  }

  /**
   * As we know the upstream and downstream node of shared queue, we don't need to put it into the
   * sink map.
   */
  public ISinkChannel createLocalSinkChannelForPipeline(
      DriverContext driverContext, String planNodeId) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodeQueryMessages.CREATE_LOCAL_SINK_HANDLE_FOR, driverContext.getDriverTaskID());
    }
    SharedTsBlockQueue queue =
        new SharedTsBlockQueue(
            driverContext.getDriverTaskID().getFragmentInstanceId().toThrift(),
            planNodeId,
            localMemoryManager,
            executorService,
            driverContext.getFragmentInstanceContext().isHighestPriority());
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
          DataNodeQueryMessages.CREATE_SINK_HANDLE_TO_PLAN_NODE,
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
        instanceContext.isHighestPriority(),
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
          DataNodeQueryMessages.SHUFFLESINKHANDLE_ALREADY_IN_MAP
              + localFragmentInstanceId
              + DataNodeQueryMessages.IS_IN_THE_MAP);
    }

    int channelNum = downStreamChannelLocationList.size();
    AtomicInteger cnt = new AtomicInteger(channelNum);
    List<ISinkChannel> downStreamChannelList =
        downStreamChannelLocationList.stream()
            .map(
                downStreamChannelLocation ->
                    createChannelForShuffleSink(
                        localFragmentInstanceId,
                        localPlanNodeId,
                        downStreamChannelLocation,
                        instanceContext,
                        cnt))
            .collect(Collectors.toList());

    ShuffleSinkHandle shuffleSinkHandle =
        new ShuffleSinkHandle(
            localFragmentInstanceId,
            downStreamChannelList,
            downStreamChannelIndex,
            shuffleStrategyEnum,
            new ShuffleSinkListenerImpl(instanceContext, instanceContext::failed));
    shuffleSinkHandles.put(localFragmentInstanceId, shuffleSinkHandle);
    return shuffleSinkHandle;
  }

  private ISinkChannel createChannelForShuffleSink(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      DownStreamChannelLocation downStreamChannelLocation,
      FragmentInstanceContext instanceContext,
      AtomicInteger cnt) {
    if (isSameNode(downStreamChannelLocation.getRemoteEndpoint())) {
      return createLocalSinkChannel(
          localFragmentInstanceId,
          downStreamChannelLocation.getRemoteFragmentInstanceId(),
          downStreamChannelLocation.getRemotePlanNodeId(),
          localPlanNodeId,
          instanceContext,
          cnt);
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
      LOGGER.debug(DataNodeQueryMessages.CREATE_LOCAL_SOURCE_HANDLE_FOR, context.getDriverTaskID());
    }
    return new PipelineSourceHandle(
        queue,
        new PipelineSourceHandleListenerImpl(context::failed),
        context.getDriverTaskID().toString());
  }

  @TestOnly
  public synchronized ISourceHandle createLocalSourceHandleForFragment(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      String remotePlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      int index,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
    return createLocalSourceHandleForFragment(
        localFragmentInstanceId,
        localPlanNodeId,
        remotePlanNodeId,
        remoteFragmentInstanceId,
        index,
        onFailureCallback,
        false);
  }

  public synchronized ISourceHandle createLocalSourceHandleForFragment(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      String remotePlanNodeId,
      TFragmentInstanceId remoteFragmentInstanceId,
      int index,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback,
      boolean isHighestPriority) {
    if (sourceHandles.containsKey(localFragmentInstanceId)
        && sourceHandles.get(localFragmentInstanceId).containsKey(localPlanNodeId)) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages.SOURCE_HANDLE_FOR_PLAN_NODE_EXISTS_FMT,
              localPlanNodeId,
              localFragmentInstanceId));
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodeQueryMessages.CREATE_LOCAL_SOURCE_HANDLE_FROM,
          remoteFragmentInstanceId,
          localPlanNodeId,
          localFragmentInstanceId);
    }

    SharedTsBlockQueue queue;
    ISinkHandle sinkHandle = shuffleSinkHandles.get(remoteFragmentInstanceId);
    if (sinkHandle != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.GET_SHARED_TSBLOCK_QUEUE_FROM_LOCAL_SINK_HANDLE);
      }
      queue = ((LocalSinkChannel) (sinkHandle.getChannel(index))).getSharedTsBlockQueue();
    } else {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(DataNodeQueryMessages.CREATE_SHARED_TSBLOCK_QUEUE);
      }
      queue =
          new SharedTsBlockQueue(
              remoteFragmentInstanceId,
              remotePlanNodeId,
              localMemoryManager,
              executorService,
              isHighestPriority);
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

  @TestOnly
  @Override
  public ISourceHandle createSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback) {
    return createSourceHandle(
        localFragmentInstanceId,
        localPlanNodeId,
        indexOfUpstreamSinkHandle,
        remoteEndpoint,
        remoteFragmentInstanceId,
        onFailureCallback,
        false);
  }

  public ISourceHandle createSourceHandle(
      TFragmentInstanceId localFragmentInstanceId,
      String localPlanNodeId,
      int indexOfUpstreamSinkHandle,
      TEndPoint remoteEndpoint,
      TFragmentInstanceId remoteFragmentInstanceId,
      IMPPDataExchangeManagerCallback<Throwable> onFailureCallback,
      boolean isHighestPriority) {
    Map<String, ISourceHandle> sourceHandleMap = sourceHandles.get(localFragmentInstanceId);
    if (sourceHandleMap != null && sourceHandleMap.containsKey(localPlanNodeId)) {
      throw new IllegalStateException(
          String.format(
              DataNodeQueryMessages.SOURCE_HANDLE_FOR_PLAN_NODE_EXISTS_FMT,
              localPlanNodeId,
              localFragmentInstanceId));
    }

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          DataNodeQueryMessages.CREATE_SOURCE_HANDLE_FROM_ARG_FOR_PLAN_NODE_ARG_OF_ARG,
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
            isHighestPriority,
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
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodeQueryMessages.START_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE);
    }
    ISink sinkHandle = shuffleSinkHandles.get(fragmentInstanceId);
    if (sinkHandle != null) {
      sinkHandle.abort();
      shuffleSinkHandles.remove(fragmentInstanceId);
    }
    Map<String, ISourceHandle> planNodeIdToSourceHandle = sourceHandles.get(fragmentInstanceId);
    if (planNodeIdToSourceHandle != null) {
      for (Entry<String, ISourceHandle> entry : planNodeIdToSourceHandle.entrySet()) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug(DataNodeQueryMessages.CLOSE_SOURCE_HANDLE, entry.getKey());
        }
        entry.getValue().abort();
      }
      sourceHandles.remove(fragmentInstanceId);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(DataNodeQueryMessages.END_FORCE_RELEASE_FI_DATA_EXCHANGE_RESOURCE);
    }
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
