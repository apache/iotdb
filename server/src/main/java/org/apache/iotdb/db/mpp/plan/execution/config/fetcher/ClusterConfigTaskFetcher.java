package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchemaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterConfigTaskFetcher implements IConfigTaskFetcher{

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
    public SettableFuture<ConfigTaskResult> setStorageGroup(SetStorageGroupStatement setStorageGroupStatement) {
        SettableFuture<ConfigTaskResult> future = SettableFuture.create();
        // Construct request using statement
        TStorageGroupSchema storageGroupSchema = constructStorageGroupSchema(setStorageGroupStatement);
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
    public SettableFuture<ConfigTaskResult> showStorageGroup(ShowStorageGroupStatement showStorageGroupStatement) {
        SettableFuture<ConfigTaskResult> future = SettableFuture.create();
        Map<String, TStorageGroupSchema> storageGroupSchemaMap = new HashMap<>();
        List<String> storageGroupPathPattern =
                Arrays.asList(showStorageGroupStatement.getPathPattern().getNodes());
        try (ConfigNodeClient client = CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
            TStorageGroupSchemaResp resp =
                    client.getMatchedStorageGroupSchemas(storageGroupPathPattern);
            storageGroupSchemaMap = resp.getStorageGroupSchemaMap();
        } catch (TException | IOException e) {
            LOGGER.error("Failed to connect to config node.");
            future.setException(e);
        }
        return future;
    }

    /** construct set storage group schema according to statement */
    private TStorageGroupSchema constructStorageGroupSchema(SetStorageGroupStatement setStorageGroupStatement) {
        TStorageGroupSchema storageGroupSchema = new TStorageGroupSchema();
        storageGroupSchema.setName(setStorageGroupStatement.getStorageGroupPath().getFullPath());
        if (setStorageGroupStatement.getTTL() != null) {
            storageGroupSchema.setTTL(setStorageGroupStatement.getTTL());
        }
        if (setStorageGroupStatement.getSchemaReplicationFactor() != null) {
            storageGroupSchema.setSchemaReplicationFactor(
                    setStorageGroupStatement.getSchemaReplicationFactor());
        }
        if (setStorageGroupStatement.getDataReplicationFactor() != null) {
            storageGroupSchema.setDataReplicationFactor(
                    setStorageGroupStatement.getDataReplicationFactor());
        }
        if (setStorageGroupStatement.getTimePartitionInterval() != null) {
            storageGroupSchema.setTimePartitionInterval(
                    setStorageGroupStatement.getTimePartitionInterval());
        }
        return storageGroupSchema;
    }
}
