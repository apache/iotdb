package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TClusterNodeInfos;
import org.apache.iotdb.confignode.rpc.thrift.TCountStorageGroupResp;
import org.apache.iotdb.confignode.rpc.thrift.TCreateFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDeleteStorageGroupsReq;
import org.apache.iotdb.confignode.rpc.thrift.TDropFunctionReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TSetTTLReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.CountStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.SetStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ShowClusterTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ShowStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ShowTTLTask;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterConfigTaskFetcher implements IConfigTaskFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandsloneConfigTaskFetcher.class);

  private static final IClientManager<PartitionRegionId, ConfigNodeClient>
      CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
              .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  private static final class ClusterConfigTaskFetcherHolder {
    private static final ClusterConfigTaskFetcher INSTANCE = new ClusterConfigTaskFetcher();

    private ClusterConfigTaskFetcherHolder() {}
  }

  public static ClusterConfigTaskFetcher getInstance() {
    return ClusterConfigTaskFetcher.ClusterConfigTaskFetcherHolder.INSTANCE;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setStorageGroup(
      SetStorageGroupStatement setStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    // Construct request using statement
    TStorageGroupSchema storageGroupSchema =
        SetStorageGroupTask.constructStorageGroupSchema(setStorageGroupStatement);
    TSetStorageGroupReq req = new TSetStorageGroupReq(storageGroupSchema);
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setStorageGroup(req);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute set storage group {} in config node, status is {}.",
            setStorageGroupStatement.getStorageGroupPath(),
            tsStatus);
        future.setException(new StatementExecutionException(tsStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, TStorageGroupSchema> storageGroupSchemaMap;
    List<String> storageGroupPathPattern =
        Arrays.asList(showStorageGroupStatement.getPathPattern().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TStorageGroupSchemaResp resp = client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
      storageGroupSchemaMap = resp.getStorageGroupSchemaMap();
      // build TSBlock
      ShowStorageGroupTask.buildTSBlock(storageGroupSchemaMap, future);
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int storageGroupNum;
    List<String> storageGroupPathPattern =
        Arrays.asList(countStorageGroupStatement.getPartialPath().getNodes());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TCountStorageGroupResp resp = client.countMatchedStorageGroups(storageGroupPathPattern);
      storageGroupNum = resp.getCount();
      // build TSBlock
      CountStorageGroupTask.buildTSBlock(storageGroupNum, future);
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createFunction(
      String udfName, String className, List<String> uris) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus =
          client.createFunction(new TCreateFunctionReq(udfName, className, uris));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error(
            "[{}] Failed to create function {}({}) in config node, URI: {}.",
            executionStatus,
            udfName,
            className,
            uris);
        future.setException(new StatementExecutionException(executionStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TDeleteStorageGroupsReq req =
        new TDeleteStorageGroupsReq(deleteStorageGroupStatement.getPrefixPath());
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TSStatus tsStatus = client.deleteStorageGroups(req);
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute delete storage group {} in config node, status is {}.",
            deleteStorageGroupStatement.getPrefixPath(),
            tsStatus);
        future.setException(new StatementExecutionException(tsStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropFunction(String udfName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      final TSStatus executionStatus = client.dropFunction(new TDropFunctionReq(udfName));

      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != executionStatus.getCode()) {
        LOGGER.error("[{}] Failed to drop function {} in config node.", executionStatus, udfName);
        future.setException(new StatementExecutionException(executionStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TSetTTLReq setTTLReq =
        new TSetTTLReq(
            setTTLStatement.getStorageGroupPath().getFullPath(), setTTLStatement.getTTL());
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TSStatus tsStatus = configNodeClient.setTTL(setTTLReq);
      // Get response or throw exception
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != tsStatus.getCode()) {
        LOGGER.error(
            "Failed to execute {} {} in config node, status is {}.",
            taskName,
            setTTLStatement.getStorageGroupPath(),
            tsStatus);
        future.setException(new StatementExecutionException(tsStatus));
      } else {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      // Send request to some API server
      TSStatus tsStatus = client.flush(tFlushReq);
      // Get response or throw exception
      if (tsStatus.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
      } else {
        future.setException(new StatementExecutionException(tsStatus));
      }
    } catch (IOException | TException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showCluster() {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    TClusterNodeInfos clusterNodeInfos = new TClusterNodeInfos();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      clusterNodeInfos = client.getAllClusterNodeInfos();
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    // build TSBlock
    ShowClusterTask.buildTSBlock(clusterNodeInfos, future);
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> storageGroupPaths = showTTLStatement.getPaths();
    Map<String, Long> storageGroupToTTL = new HashMap<>();
    try (ConfigNodeClient client =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      if (showTTLStatement.isAll()) {
        List<String> allStorageGroupPathPattern = Arrays.asList("root", "**");
        TStorageGroupSchemaResp resp =
            client.getMatchedStorageGroupSchemas(allStorageGroupPathPattern);
        for (Map.Entry<String, TStorageGroupSchema> entry :
            resp.getStorageGroupSchemaMap().entrySet()) {
          storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
        }
      } else {
        for (PartialPath storageGroupPath : storageGroupPaths) {
          List<String> storageGroupPathPattern = Arrays.asList(storageGroupPath.getNodes());
          TStorageGroupSchemaResp resp =
              client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
          for (Map.Entry<String, TStorageGroupSchema> entry :
              resp.getStorageGroupSchemaMap().entrySet()) {
            if (!storageGroupToTTL.containsKey(entry.getKey())) {
              storageGroupToTTL.put(entry.getKey(), entry.getValue().getTTL());
            }
          }
        }
      }
    } catch (TException | IOException e) {
      LOGGER.error("Failed to connect to config node.");
      future.setException(e);
    }
    // build TSBlock
    ShowTTLTask.buildTSBlock(storageGroupToTTL, future);
    return future;
  }
}
