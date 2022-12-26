package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.node.heartbeat.BaseNodeCache;
import org.apache.iotdb.mpp.rpc.thrift.TOperatePipeOnDataNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FailedTasksRetryThread {

  private static final Logger LOGGER = LoggerFactory.getLogger(FailedTasksRetryThread.class);

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private static final long HEARTBEAT_INTERVAL = CONF.getHeartbeatIntervalInMs();
  private final IManager configManager;
  private final NodeManager nodeManager;
  private final Set<TDataNodeLocation> oldUnknownNodes;
  private final ScheduledExecutorService retryFailMissionsExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor("Cluster-RetryFailMissions-Service");
  private final Object scheduleMonitor = new Object();
  private Future<?> currentLoadStatisticsFuture;
  private final Map<Integer, Queue<TOperatePipeOnDataNodeReq>> messageMap =
      new ConcurrentHashMap<>();

  public FailedTasksRetryThread(IManager configManager) {
    this.configManager = configManager;
    this.nodeManager = configManager.getNodeManager();
    this.oldUnknownNodes = new HashSet<>();
  }

  public void startRetryFailTasksService() {
    synchronized (scheduleMonitor) {
      if (currentLoadStatisticsFuture == null) {
        currentLoadStatisticsFuture =
            ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
                retryFailMissionsExecutor,
                this::retryFailMissions,
                0,
                HEARTBEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        LOGGER.info("RetryFailMissions service is started successfully.");
      }
    }
  }

  /** Stop the retry fail missions service */
  public void stopRetryFailTasksService() {
    synchronized (scheduleMonitor) {
      if (currentLoadStatisticsFuture != null) {
        currentLoadStatisticsFuture.cancel(false);
        currentLoadStatisticsFuture = null;
        LOGGER.info("RetryFailMissions service is stopped successfully.");
      }
    }
  }

  public void retryRollbackReq(List<Integer> dataNodeIds, TOperatePipeOnDataNodeReq req) {
    for (int id : dataNodeIds) {
      messageMap.computeIfAbsent(id, i -> new LinkedList<>()).add(req);
    }
  }

  private void retryFailMissions() {
    // trigger
    triggerDetectTask();

    // sync
    syncDetectTask();
  }

  private void syncDetectTask() {
    LOGGER.info("Scheduled OperatePipeProcedureRollbackProcessor.");
    for (Map.Entry<Integer, Queue<TOperatePipeOnDataNodeReq>> entry : messageMap.entrySet()) {
      int dataNodeId = entry.getKey();
      if (NodeStatus.Running.equals(nodeManager.getNodeStatusByNodeId(dataNodeId))) {
        final Map<Integer, TDataNodeLocation> dataNodeLocationMap = new HashMap<>();
        dataNodeLocationMap.put(
            dataNodeId, nodeManager.getRegisteredDataNodeLocations().get(dataNodeId));
        TOperatePipeOnDataNodeReq request;
        while ((request = entry.getValue().peek()) != null) {
          AsyncClientHandler<TOperatePipeOnDataNodeReq, TSStatus> clientHandler =
              new AsyncClientHandler<>(
                  DataNodeRequestType.ROLLBACK_OPERATE_PIPE, request, dataNodeLocationMap);
          AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
          TSStatus tsStatus = clientHandler.getResponseList().get(0);
          if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            entry.getValue().poll();
          } else if (tsStatus.getCode() == TSStatusCode.PIPE_ERROR.getStatusCode()) {
            // skip
            LOGGER.warn(
                String.format(
                    "Roll back failed because %s. Skip this roll back request [%s].",
                    tsStatus.getMessage(), request));
          } else {
            // connection failure, keep and retry.
            LOGGER.error(
                String.format(
                    "Roll back failed because %s. This roll back request [%s] will be retried later.",
                    tsStatus.getMessage(), request));
            break;
          }
        }
      }
    }
  }

  /**
   * The detectTask executed periodically to find newest UnknownDataNodes
   *
   * <p>1.If one DataNode is continuing Unknown, we shouldn't always activate Transfer of this Node.
   *
   * <p>2.The selected DataNodes may not truly need to transfer, so you should ensure safety of the
   * Data when implement transferMethod in Manager.
   */
  private void triggerDetectTask() {
    List<TDataNodeLocation> newUnknownNodes = new ArrayList<>();

    nodeManager
        .getRegisteredDataNodes()
        .forEach(
            DataNodeConfiguration -> {
              TDataNodeLocation dataNodeLocation = DataNodeConfiguration.getLocation();
              BaseNodeCache newestNodeInformation =
                  nodeManager.getNodeCacheMap().get(dataNodeLocation.dataNodeId);
              if (newestNodeInformation != null) {
                if (newestNodeInformation.getNodeStatus() == NodeStatus.Running) {
                  oldUnknownNodes.remove(dataNodeLocation);
                } else if (!oldUnknownNodes.contains(dataNodeLocation)
                    && newestNodeInformation.getNodeStatus() == NodeStatus.Unknown) {
                  newUnknownNodes.add(dataNodeLocation);
                }
              }
            });

    if (!newUnknownNodes.isEmpty()) {
      TSStatus transferResult = configManager.transfer(newUnknownNodes);
      if (transferResult.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        oldUnknownNodes.addAll(newUnknownNodes);
      }
    }
  }
}
