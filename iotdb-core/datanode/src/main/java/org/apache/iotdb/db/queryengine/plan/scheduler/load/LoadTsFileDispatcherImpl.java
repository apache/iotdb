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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferErrorCode;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.audit.DataNodeUserDataTransferAuditor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.load.LoadFileException;
import org.apache.iotdb.db.exception.mpp.FragmentInstanceDispatchException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.SubPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadTsFilePieceNode;
import org.apache.iotdb.db.queryengine.plan.scheduler.FragInstanceDispatchResult;
import org.apache.iotdb.db.queryengine.plan.scheduler.IFragInstanceDispatcher;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.TableDiskUsageStatisticUtil;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.mpp.rpc.thrift.TLoadResp;
import org.apache.iotdb.mpp.rpc.thrift.TTsFilePieceReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dispatches LOAD FragmentInstances locally or remotely across replica DataNodes. Handles adaptive
 * piece-splitting based on negotiated RPC frame limits and connection timeouts.
 */
public class LoadTsFileDispatcherImpl implements IFragInstanceDispatcher, AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileDispatcherImpl.class);

  private static final int MAX_CONNECTION_TIMEOUT_MS = 24 * 60 * 60 * 1000; // 1 day
  private static final int FIRST_ADJUSTMENT_TIMEOUT_MS = 6 * 60 * 60 * 1000; // 6 hours
  private static final int LOAD_TSFILE_PIECE_RPC_FRAME_RESERVED_BYTES = 1024;

  private static final AtomicInteger CONNECTION_TIMEOUT_MS =
      new AtomicInteger(IoTDBDescriptor.getInstance().getConfig().getConnectionTimeoutInMS());

  private final String localhostIpAddr;
  private final int localhostInternalPort;
  private final TEndPoint localEndPoint;
  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;
  private final boolean isGeneratedByPipe;
  private final Map<TEndPoint, Integer> endPoint2ThriftMaxFrameSize = new ConcurrentHashMap<>();

  private volatile String uuid;
  private volatile ExecutorService executor;

  public LoadTsFileDispatcherImpl(
      final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager,
      final boolean isGeneratedByPipe) {
    this.clientManager =
        Objects.requireNonNull(
            clientManager, DataNodeQueryMessages.EXCEPTION_CLIENTMANAGER_CANNOT_BE_NULL_FAF70317);
    this.localhostIpAddr = IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
    this.localhostInternalPort = IoTDBDescriptor.getInstance().getConfig().getInternalPort();
    this.localEndPoint = new TEndPoint(localhostIpAddr, localhostInternalPort);
    this.isGeneratedByPipe = isGeneratedByPipe;
  }

  public void setUuid(final String uuid) {
    this.uuid = uuid;
  }

  // -------------------------------------------------------------------------
  // Dispatcher Lifecycle & Thread Management
  // -------------------------------------------------------------------------

  private ExecutorService getOrCreateExecutor() {
    ExecutorService localExecutor = executor;
    if (localExecutor == null || localExecutor.isShutdown()) {
      synchronized (this) {
        localExecutor = executor;
        if (localExecutor == null || localExecutor.isShutdown()) {
          localExecutor =
              IoTDBThreadPoolFactory.newCachedThreadPool(LoadTsFileDispatcherImpl.class.getName());
          executor = localExecutor;
        }
      }
    }
    return localExecutor;
  }

  @Override
  public Future<FragInstanceDispatchResult> dispatch(
      final SubPlan root, final List<FragmentInstance> instances) {
    return getOrCreateExecutor()
        .submit(
            () -> {
              for (final FragmentInstance instance : instances) {
                final String threadContext =
                    "load-dispatcher-" + instance.getId().getFullId() + "-" + uuid;
                try (final SetThreadName threadName = new SetThreadName(threadContext)) {
                  dispatchOneInstance(instance);
                } catch (final FragmentInstanceDispatchException e) {
                  return new FragInstanceDispatchResult(e.getFailureStatus());
                } catch (final Exception t) {
                  LOGGER.warn(DataNodeQueryMessages.CANNOT_DISPATCH_FI_FOR_LOAD_OPERATION, t);
                  return new FragInstanceDispatchResult(
                      RpcUtils.getStatus(
                          TSStatusCode.INTERNAL_SERVER_ERROR,
                          String.format(
                              DataNodeQueryMessages.MESSAGE_UNEXPECTED_ERRORS_ARG_78EE0800,
                              t.getMessage())));
                }
              }
              return new FragInstanceDispatchResult(true);
            });
  }

  private void dispatchOneInstance(final FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    ByteBuffer cachedSerializedBody = null;

    for (final TDataNodeLocation location : instance.getRegionReplicaSet().getDataNodeLocations()) {
      final TEndPoint endPoint = location.getInternalEndPoint();
      if (isDispatchedToLocal(endPoint)) {
        dispatchLocally(instance);
      } else {
        if (cachedSerializedBody == null) {
          cachedSerializedBody = instance.getFragment().getPlanNodeTree().serializeToByteBuffer();
        }
        dispatchRemote(
            cachedSerializedBody, instance.getRegionReplicaSet().getRegionId(), endPoint);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Local Dispatch Strategy
  // -------------------------------------------------------------------------

  public void dispatchLocally(final FragmentInstance instance)
      throws FragmentInstanceDispatchException {
    if (isGeneratedByPipe) {
      LOGGER.debug(DataNodeQueryMessages.RECEIVE_LOAD_NODE_FROM_UUID, uuid);
    } else {
      LOGGER.info(DataNodeQueryMessages.RECEIVE_LOAD_NODE_FROM_UUID, uuid);
    }

    final ConsensusGroupId groupId =
        ConsensusGroupId.Factory.createFromTConsensusGroupId(
            instance.getRegionReplicaSet().getRegionId());
    final PlanNode planNode = instance.getFragment().getPlanNodeTree();

    if (planNode instanceof LoadTsFilePieceNode pieceNode) {
      final TSStatus status =
          StorageEngine.getInstance().writeLoadTsFileNode((DataRegionId) groupId, pieceNode, uuid);
      if (!RpcUtils.SUCCESS_STATUS.equals(status)) {
        throw new FragmentInstanceDispatchException(status);
      }
    } else if (planNode instanceof LoadSingleTsFileNode singleNode) {
      executeLocalSingleTsFileLoad(groupId, singleNode);
    }
  }

  private void executeLocalSingleTsFileLoad(
      final ConsensusGroupId groupId, final LoadSingleTsFileNode singleNode)
      throws FragmentInstanceDispatchException {
    final TsFileResource resource = singleNode.getTsFileResource();
    final String filePath = resource.getTsFile().getAbsolutePath();

    try {
      PipeDataNodeAgent.runtime().assignProgressIndexForTsFileLoad(resource);
      resource.setGeneratedByPipe(isGeneratedByPipe);
      resource.serialize();

      // Not final: the variable is assigned in the try block and again in the catch clause
      TsFileResource clonedResource;
      try {
        clonedResource = resource.shallowCloneForNative();
      } catch (final CloneNotSupportedException e) {
        clonedResource = resource.shallowClone();
      }

      final DataRegion dataRegion =
          StorageEngine.getInstance().getDataRegion((DataRegionId) groupId);
      dataRegion.loadNewTsFile(
          clonedResource,
          singleNode.isDeleteAfterLoad(),
          isGeneratedByPipe,
          false,
          dataRegion.isTableModel()
              ? TableDiskUsageStatisticUtil.calculateTableSizeMap(clonedResource)
              : Optional.empty());
    } catch (final LoadFileException e) {
      LOGGER.warn(DataNodeQueryMessages.LOAD_TSFILE_NODE_ERROR, singleNode, e);
      throw new FragmentInstanceDispatchException(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    } catch (final IOException e) {
      LOGGER.warn(DataNodeQueryMessages.SERIALIZE_TSFILERESOURCE_ERROR, filePath, e);
      throw new FragmentInstanceDispatchException(
          new TSStatus(TSStatusCode.LOAD_FILE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    }
  }

  // -------------------------------------------------------------------------
  // Remote Dispatch & Slicing Protocol
  // -------------------------------------------------------------------------

  private void dispatchRemote(
      final ByteBuffer body, final TConsensusGroupId consensusGroupId, final TEndPoint endPoint)
      throws FragmentInstanceDispatchException {
    boolean transferAttemptAudited = false;
    try {
      final int bodySizeLimit = getLoadTsFilePieceBodySizeLimit(endPoint);
      final List<TTsFilePieceReq> requests =
          splitTsFilePieceReq(body, uuid, consensusGroupId, bodySizeLimit);

      try (final SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        client.setTimeout(CONNECTION_TIMEOUT_MS.get());

        for (final TTsFilePieceReq request : requests) {
          final TLoadResp loadResp = client.sendTsFilePieceNode(request);
          if (!loadResp.isAccepted()) {
            final String errorCode =
                loadResp.isSetStatus()
                    ? String.valueOf(loadResp.getStatus().getCode())
                    : UserDataTransferErrorCode.REMOTE_REJECTED.name();
            recordTransferAttempt(endPoint, false, errorCode, null);
            transferAttemptAudited = true;
            LOGGER.warn(loadResp.getMessage());
            throw new FragmentInstanceDispatchException(loadResp.getStatus());
          }
        }
        recordTransferAttempt(endPoint, true, null, null);
        transferAttemptAudited = true;
      }
    } catch (final Exception e) {
      if (!transferAttemptAudited) {
        recordTransferAttempt(endPoint, false, null, e);
      }
      adjustTimeoutIfNecessary(e);

      final String message =
          String.format(
              DataNodeQueryMessages
                  .MESSAGE_FAILED_TO_DISPATCH_LOAD_COMMAND_ARG_TO_NODE_ARG_BECAUSE_OF_EXCEPTION_ARG_2D8A483D,
              uuid,
              endPoint,
              e);
      LOGGER.warn(message, e);
      throw new FragmentInstanceDispatchException(
          new TSStatus(TSStatusCode.DISPATCH_ERROR.getStatusCode()).setMessage(message));
    }
  }

  private int getLoadTsFilePieceBodySizeLimit(final TEndPoint endPoint) {
    final int localMaxFrameSize = IoTDBDescriptor.getInstance().getConfig().getThriftMaxFrameSize();
    final int remoteMaxFrameSize =
        endPoint2ThriftMaxFrameSize.computeIfAbsent(
            endPoint,
            ep -> {
              try (final SyncDataNodeInternalServiceClient client =
                  clientManager.borrowClient(ep)) {
                final int frameSize = client.getThriftMaxFrameSize();
                if (frameSize <= 0) {
                  throw new IllegalArgumentException(
                      String.format(
                          DataNodeQueryMessages
                              .EXCEPTION_INVALID_THRIFT_MAXIMUM_FRAME_SIZE_ARG_FROM_ARG_A639588B,
                          frameSize,
                          ep));
                }
                return frameSize;
              } catch (final Exception e) {
                if (isUnknownMethod(e)) {
                  return localMaxFrameSize;
                }
                throw new IllegalStateException(
                    String.format(
                        DataNodeQueryMessages
                            .EXCEPTION_FAILED_TO_QUERY_FRAME_SIZE_FROM_ARG_4A82B38D,
                        ep),
                    e);
              }
            });

    return Math.max(
        1,
        Math.min(localMaxFrameSize, remoteMaxFrameSize)
            - LOAD_TSFILE_PIECE_RPC_FRAME_RESERVED_BYTES);
  }

  static List<TTsFilePieceReq> splitTsFilePieceReq(
      final ByteBuffer body,
      final String uuid,
      final TConsensusGroupId consensusGroupId,
      final int bodySizeLimit) {
    if (bodySizeLimit <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_BODYSIZELIMIT_MUST_BE_POSITIVE_EECFA175);
    }

    final int originBodySize = body.remaining();
    final int sliceCount = getSliceCount(originBodySize, bodySizeLimit);
    final List<TTsFilePieceReq> requests = new ArrayList<>(sliceCount);

    if (sliceCount == 1) {
      requests.add(createTsFilePieceReq(body.duplicate(), uuid, consensusGroupId));
      return requests;
    }

    final int originPosition = body.position();
    for (int sliceIndex = 0; sliceIndex < sliceCount; sliceIndex++) {
      final int startOffset = sliceIndex * bodySizeLimit;
      final int endOffset = startOffset + Math.min(bodySizeLimit, originBodySize - startOffset);

      final ByteBuffer slicedBody = body.duplicate();
      slicedBody.position(originPosition + startOffset);
      slicedBody.limit(originPosition + endOffset);

      requests.add(
          createTsFilePieceReq(slicedBody.slice(), uuid, consensusGroupId)
              .setSliceIndex(sliceIndex)
              .setSliceCount(sliceCount)
              .setOriginBodySize(originBodySize));
    }
    return requests;
  }

  static int getSliceCount(final int bodySize, final int bodySizeLimit) {
    if (bodySize < 0 || bodySizeLimit <= 0) {
      throw new IllegalArgumentException(
          DataNodeQueryMessages.EXCEPTION_INVALID_SLICE_COUNT_ARGUMENTS_5ADCE8CC);
    }
    return bodySize == 0 ? 1 : (bodySize - 1) / bodySizeLimit + 1;
  }

  private static TTsFilePieceReq createTsFilePieceReq(
      final ByteBuffer body, final String uuid, final TConsensusGroupId consensusGroupId) {
    final TTsFilePieceReq request =
        new TTsFilePieceReq().setUuid(uuid).setConsensusGroupId(consensusGroupId);
    request.body = body;
    return request;
  }

  // -------------------------------------------------------------------------
  // Helpers & Error Handlers
  // -------------------------------------------------------------------------

  private boolean isDispatchedToLocal(final TEndPoint endPoint) {
    return this.localhostIpAddr.equals(endPoint.getIp())
        && this.localhostInternalPort == endPoint.getPort();
  }

  private void recordTransferAttempt(
      final TEndPoint target,
      final boolean success,
      final String errorCode,
      final Throwable error) {
    DataNodeUserDataTransferAuditor.record(
        localEndPoint, localEndPoint, target, success, errorCode, error);
  }

  private static boolean isUnknownMethod(Throwable e) {
    while (e != null) {
      if (e instanceof TApplicationException appEx
          && appEx.getType() == TApplicationException.UNKNOWN_METHOD) {
        return true;
      }
      e = e.getCause();
    }
    return false;
  }

  private static void adjustTimeoutIfNecessary(Throwable e) {
    while (e != null) {
      if (e instanceof SocketTimeoutException || e instanceof TimeoutException) {
        final int updatedTimeout =
            CONNECTION_TIMEOUT_MS.updateAndGet(
                current -> {
                  try {
                    return Math.min(
                        Math.max(FIRST_ADJUSTMENT_TIMEOUT_MS, Math.toIntExact(current * 2L)),
                        MAX_CONNECTION_TIMEOUT_MS);
                  } catch (final ArithmeticException ignored) {
                    return MAX_CONNECTION_TIMEOUT_MS;
                  }
                });

        LOGGER.info(
            DataNodeQueryMessages
                .LOAD_REMOTE_PROCEDURE_CALL_CONNECTION_TIMEOUT_IS_ADJUSTED_TO_ARG_MS_ARG_MINS,
            updatedTimeout,
            updatedTimeout / 60000.0);
        return;
      }
      e = e.getCause();
    }
  }

  @Override
  public void abort() {
    close();
  }

  @Override
  public synchronized void close() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }
}
