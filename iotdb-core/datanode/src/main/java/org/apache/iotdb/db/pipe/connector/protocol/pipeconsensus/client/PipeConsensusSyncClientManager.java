package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.payload.pipeconsensus.request.PipeConsensusHandshakeReq;
import org.apache.iotdb.commons.pipe.connector.payload.thrift.common.PipeTransferHandshakeConstant;
import org.apache.iotdb.mpp.rpc.thrift.TPipeConsensusTransferResp;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is a simplified version of {@link
 * org.apache.iotdb.db.pipe.connector.client.IoTDBDataNodeSyncClientManager}, because pipeConsensus
 * currently only needs to reuse the handshake function.
 *
 * <p>Note: This class is shared by all pipeConsensusTasks of one leader to its peers in a consensus
 * group.
 */
public class PipeConsensusSyncClientManager {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusSyncClientManager.class);

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> SYNC_RETRY_CLIENT_MGR =
      new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
          .createClientManager(
              new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  protected final Map<TEndPoint, Pair<SyncDataNodeInternalServiceClient, Boolean>>
      endPoint2ClientAndStatus = new ConcurrentHashMap<>();

  private PipeConsensusSyncClientManager() {
    // do nothing
  }

  public SyncDataNodeInternalServiceClient borrowClient(final TEndPoint endPoint) {
    Pair<SyncDataNodeInternalServiceClient, Boolean> clientAndStatus =
        endPoint2ClientAndStatus.getOrDefault(endPoint, null);
    // If the client don't exist due to eviction by ClientManager's KeyObjectPool or other reasons,
    // it needs to be reconstructed and handshake
    if (clientAndStatus == null) {
      clientAndStatus = new Pair<>(null, false);
      initClientAndStatus(clientAndStatus, endPoint);
      sendHandshakeReq(clientAndStatus);
      endPoint2ClientAndStatus.putIfAbsent(endPoint, clientAndStatus);
    }
    return clientAndStatus.getLeft();
  }

  /**
   * Among all peers on each leader, recreate dead clients and see if any peer client is available
   */
  public void checkClientStatusAndTryReconstructIfNecessary(List<TEndPoint> peers) {
    // Reconstruct all dead clients
    endPoint2ClientAndStatus.entrySet().stream()
        .filter(
            entry ->
                Boolean.FALSE.equals(entry.getValue().getRight()) && peers.contains(entry.getKey()))
        .forEach(entry -> reconstructClient(entry.getKey()));

    // Check whether any peers clients are available
    for (final Pair<SyncDataNodeInternalServiceClient, Boolean> clientAndStatus :
        endPoint2ClientAndStatus.values()) {
      if (Boolean.TRUE.equals(clientAndStatus.getRight())) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format(
            "All target servers %s are not available.", endPoint2ClientAndStatus.keySet()));
  }

  protected void reconstructClient(TEndPoint endPoint) {
    final Pair<SyncDataNodeInternalServiceClient, Boolean> clientAndStatus =
        endPoint2ClientAndStatus.get(endPoint);

    if (clientAndStatus.getLeft() != null) {
      try {
        clientAndStatus.getLeft().invalidate();
      } catch (Exception e) {
        LOGGER.warn(
            "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
            endPoint.getIp(),
            endPoint.getPort(),
            e.getMessage());
      }
    }

    initClientAndStatus(clientAndStatus, endPoint);
    sendHandshakeReq(clientAndStatus);
  }

  private void initClientAndStatus(
      final Pair<SyncDataNodeInternalServiceClient, Boolean> clientAndStatus,
      final TEndPoint endPoint) {
    try {
      clientAndStatus.setLeft(SYNC_RETRY_CLIENT_MGR.borrowClient(endPoint));
    } catch (ClientManagerException e) {
      throw new PipeConnectionException(
          String.format(
              PipeConnectionException.CONNECTION_ERROR_FORMATTER,
              endPoint.getIp(),
              endPoint.getPort()),
          e);
    }
  }

  public void sendHandshakeReq(
      final Pair<SyncDataNodeInternalServiceClient, Boolean> clientAndStatus) {
    final SyncDataNodeInternalServiceClient client = clientAndStatus.getLeft();
    try {
      final HashMap<String, String> params = new HashMap<>();
      params.put(
          PipeTransferHandshakeConstant.HANDSHAKE_KEY_TIME_PRECISION,
          CommonDescriptor.getInstance().getConfig().getTimestampPrecision());

      // Try to handshake by PipeConsensusHandshakeReq
      PipeConsensusHandshakeReq handshakeReq = new PipeConsensusHandshakeReq();
      TPipeConsensusTransferResp resp =
          client.pipeConsensusTransfer(handshakeReq.convertToTPipeTransferReq(params));

      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.warn(
            "PipeConsensus handshake error with target server ip: {}, port: {}, because: {}.",
            client.getTEndpoint().getIp(),
            client.getTEndpoint().getPort(),
            resp.getStatus());
      } else {
        clientAndStatus.setRight(true);
        client.setTimeout((int) PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());
        LOGGER.info(
            "PipeConsensus handshake success. Target server ip: {}, port: {}",
            client.getTEndpoint().getIp(),
            client.getTEndpoint().getPort());
      }
    } catch (Exception e) {
      LOGGER.warn(
          "PipeConsensus handshake error with target server ip: {}, port: {}, because: {}.",
          client.getTEndpoint().getIp(),
          client.getTEndpoint().getPort(),
          e.getMessage(),
          e);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConsensusSyncClientManagerHolder {
    private static final PipeConsensusSyncClientManager INSTANCE =
        new PipeConsensusSyncClientManager();

    private PipeConsensusSyncClientManagerHolder() {}

    /** Add one leader's own peers to the manager */
    private static void construct(List<TEndPoint> peers) {
      for (final TEndPoint endPoint : peers) {
        INSTANCE.endPoint2ClientAndStatus.putIfAbsent(endPoint, new Pair<>(null, false));
      }
    }
  }

  public static PipeConsensusSyncClientManager getInstance() {
    return PipeConsensusSyncClientManagerHolder.INSTANCE;
  }

  public static PipeConsensusSyncClientManager onPeers(List<TEndPoint> peers) {
    PipeConsensusSyncClientManagerHolder.construct(peers);
    return PipeConsensusSyncClientManagerHolder.INSTANCE;
  }
}
