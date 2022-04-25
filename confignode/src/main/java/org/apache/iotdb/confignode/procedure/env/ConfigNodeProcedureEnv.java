package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.ConfigNodeClientPoolFactory;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);
  // init syncClientManager
  ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> syncClusterManager =
      (ClientManager<TEndPoint, SyncDataNodeInternalServiceClient>)
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new ConfigNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  public InternalService.Client getDataNodeClient(TEndPoint endPoint) {
    InternalService.Client client = null;
    try {
      client = syncClusterManager.getPool().borrowObject(endPoint);
    } catch (Exception e) {
      LOG.error("Get DN client failed.", e);
    }
    return client;
  }
}
