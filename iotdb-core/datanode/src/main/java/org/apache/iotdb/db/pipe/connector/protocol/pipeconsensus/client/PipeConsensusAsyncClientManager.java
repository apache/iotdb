package org.apache.iotdb.db.pipe.connector.protocol.pipeconsensus.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeConsensusAsyncClientManager {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(PipeConsensusAsyncClientManager.class);

  private final IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient>
      ASYNC_TRANSFER_CLIENT_MGR =
          new IClientManager.Factory<TEndPoint, AsyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ClientPoolFactory.AsyncDataNodeInternalServiceClientPoolFactory());

  private PipeConsensusAsyncClientManager() {
    // do nothing
  }

  // TODO: Since each peer will have a sync handshake in advance, do we need to handshake the async
  // client again?
  public AsyncDataNodeInternalServiceClient borrowClient(final TEndPoint endPoint) {
    try {
      return ASYNC_TRANSFER_CLIENT_MGR.borrowClient(endPoint);
    } catch (ClientManagerException e) {
      throw new PipeConnectionException(
          String.format(
              PipeConnectionException.CONNECTION_ERROR_FORMATTER,
              endPoint.getIp(),
              endPoint.getPort()),
          e);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeConsensusAsyncClientManagerHolder {
    private static final PipeConsensusAsyncClientManager INSTANCE =
        new PipeConsensusAsyncClientManager();

    private PipeConsensusAsyncClientManagerHolder() {}
  }

  public static PipeConsensusAsyncClientManager getInstance() {
    return PipeConsensusAsyncClientManagerHolder.INSTANCE;
  }
}
