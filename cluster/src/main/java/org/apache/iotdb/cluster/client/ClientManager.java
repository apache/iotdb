package org.apache.iotdb.cluster.client;

import org.apache.iotdb.cluster.ClusterIoTDB;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.async.AsyncMetaClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncMetaClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import com.google.common.collect.Maps;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * One should borrow the reusable client from this class and return the client after use. The
 * underlying client pool is powered by Apache Commons Pool. The class provided 3 default pool group
 * according to current usage: ClusterClients, DataGroupClients, MetaGroupClients.
 *
 * <p>ClusterClients implement the data query and insert interfaces such as query and non-query
 * request
 *
 * <p>DataGroupClient implement the data group raft rpc interfaces such as appendEntry,
 * appendEntries, sendHeartbeat, etc.
 *
 * <p>MetaGroupClient implement the meta group raft rpc interfaces such as appendEntry,
 * appendEntries, sendHeartbeat, etc.
 *
 * <p>TODO: We can refine the client structure by reorg the interfaces defined in cluster-thrift.
 */
public class ClientManager implements IClientPool {

  private static final Logger logger = LoggerFactory.getLogger(ClusterIoTDB.class);

  private Map<ClientCategory, KeyedObjectPool<Node, RaftService.AsyncClient>> asyncClientPoolMap;
  private Map<ClientCategory, KeyedObjectPool<Node, RaftService.Client>> syncClientPoolMap;

  public enum Type {
    ClusterClient,
    DataGroupClient,
    MetaGroupClient
  }

  public ClientManager(boolean isAsyncMode, Type type) {
    if (isAsyncMode) {
      asyncClientPoolMap = Maps.newHashMap();
      constructAsyncClientMap(type);
    } else {
      syncClientPoolMap = Maps.newHashMap();
      constructSyncClientMap(type);
    }
  }

  private void constructAsyncClientMap(Type type) {
    switch (type) {
      case ClusterClient:
        asyncClientPoolMap.put(
            ClientCategory.DATA,
            ClientPoolFactory.getInstance().createAsyncDataPool(ClientCategory.DATA));
        break;
      case MetaGroupClient:
        asyncClientPoolMap.put(
            ClientCategory.META,
            ClientPoolFactory.getInstance().createAsyncMetaPool(ClientCategory.META));
        asyncClientPoolMap.put(
            ClientCategory.META_HEARTBEAT,
            ClientPoolFactory.getInstance().createAsyncMetaPool(ClientCategory.META_HEARTBEAT));
        break;
      case DataGroupClient:
        asyncClientPoolMap.put(
            ClientCategory.DATA,
            ClientPoolFactory.getInstance().createAsyncDataPool(ClientCategory.DATA));
        asyncClientPoolMap.put(
            ClientCategory.DATA_HEARTBEAT,
            ClientPoolFactory.getInstance().createAsyncDataPool(ClientCategory.DATA_HEARTBEAT));
        asyncClientPoolMap.put(
            ClientCategory.SINGLE_MASTER,
            ClientPoolFactory.getInstance().createSingleManagerAsyncDataPool());
        break;
      default:
        break;
    }
  }

  private void constructSyncClientMap(Type type) {
    switch (type) {
      case ClusterClient:
        syncClientPoolMap.put(
            ClientCategory.DATA,
            ClientPoolFactory.getInstance().createSyncDataPool(ClientCategory.DATA));
        break;
      case MetaGroupClient:
        syncClientPoolMap.put(
            ClientCategory.META,
            ClientPoolFactory.getInstance().createSyncMetaPool(ClientCategory.META));
        syncClientPoolMap.put(
            ClientCategory.META_HEARTBEAT,
            ClientPoolFactory.getInstance().createSyncMetaPool(ClientCategory.META_HEARTBEAT));
        break;
      case DataGroupClient:
        syncClientPoolMap.put(
            ClientCategory.DATA,
            ClientPoolFactory.getInstance().createSyncDataPool(ClientCategory.DATA));
        syncClientPoolMap.put(
            ClientCategory.DATA_HEARTBEAT,
            ClientPoolFactory.getInstance().createSyncDataPool(ClientCategory.DATA_HEARTBEAT));
        break;
      default:
        break;
    }
  }

  /**
   * It's safe to convert: 1. RaftService.AsyncClient to TSDataService.AsyncClient when category is
   * DATA or DATA_HEARTBEAT; 2. RaftService.AsyncClient to TSMetaService.AsyncClient when category
   * is META or META_HEARTBEAT.
   *
   * @param category
   * @return RaftService.AsyncClient
   */
  @Override
  public RaftService.AsyncClient borrowAsyncClient(Node node, ClientCategory category) {
    try {
      RaftService.AsyncClient client = asyncClientPoolMap.get(category).borrowObject(node);
      if (ClientCategory.DATA == category
          || ClientCategory.DATA_HEARTBEAT == category
          || ClientCategory.SINGLE_MASTER == category) {
        ((AsyncDataClient) client).setClientPool(this);
      } else {
        ((AsyncMetaClient) client).setClientPool(this);
      }
      return client;
    } catch (NullPointerException e) {
      logger.error("No AsyncClient pool found for {}", category, e);
    } catch (TException e) {
      logger.error("AsyncClient transport error for {}", category, e);
    } catch (Exception e) {
      logger.error("AsyncClient error for {}", category, e);
    }
    return null;
  }

  /**
   * It's safe to convert: 1. RaftService.Client to TSDataService.Client when category is DATA or
   * DATA_HEARTBEAT; 2. RaftService.Client to TSMetaService.Client when category is META or
   * META_HEARTBEAT.
   *
   * @param category
   * @return RaftService.Client
   */
  @Override
  public RaftService.Client borrowSyncClient(Node node, ClientCategory category) {
    try {
      RaftService.Client client = syncClientPoolMap.get(category).borrowObject(node);
      if (ClientCategory.DATA == category || ClientCategory.DATA_HEARTBEAT == category) {
        ((SyncDataClient) client).setClientPool(this);
      } else if (ClientCategory.META == category || ClientCategory.META_HEARTBEAT == category) {
        ((SyncMetaClient) client).setClientPool(this);
      } else {
        throw new UnsupportedOperationException("SINGLE_MASTER type can't be sync");
      }
      return client;
    } catch (NullPointerException e) {
      logger.error("No SyncClient pool found for {}", category, e);
    } catch (TException e) {
      logger.error("SyncClient transport error for {}", category, e);
    } catch (Exception e) {
      logger.error("SyncClient error for {}", category, e);
    }
    return null;
  }

  // TODO: reset returned client's timeout property as it may be changed outside
  @Override
  public void returnAsyncClient(
      RaftService.AsyncClient client, Node node, ClientCategory category) {
    if (client != null && node != null) {
      try {
        asyncClientPoolMap.get(category).returnObject(node, client);
      } catch (Exception e) {
        logger.error("AsyncClient return error: {}", client, e);
      }
    }
  }

  // TODO: reset returned client's timeout property as it may be changed outside
  @Override
  public void returnSyncClient(RaftService.Client client, Node node, ClientCategory category) {
    if (client != null && node != null) {
      try {
        syncClientPoolMap.get(category).returnObject(node, client);
      } catch (Exception e) {
        logger.error("SyncClient return error: {}", client, e);
      }
    }
  }
}
