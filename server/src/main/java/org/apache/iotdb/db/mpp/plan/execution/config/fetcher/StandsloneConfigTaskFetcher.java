package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandsloneConfigTaskFetcher implements IConfigTaskFetcher{

    private static final Logger LOGGER = LoggerFactory.getLogger(StandsloneConfigTaskFetcher.class);

    private static final class StandsloneConfigTaskFetcherHolder {
        private static final StandsloneConfigTaskFetcher INSTANCE = new StandsloneConfigTaskFetcher();

        private StandsloneConfigTaskFetcherHolder() {}
    }

    public static StandsloneConfigTaskFetcher getInstance() {
        return StandsloneConfigTaskFetcher.StandsloneConfigTaskFetcherHolder.INSTANCE;
    }

    @Override
    public SettableFuture<ConfigTaskResult> setStorageGroup(SetStorageGroupStatement setStorageGroupStatement) {
        SettableFuture<ConfigTaskResult> future = SettableFuture.create();
        try {
            LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
            localConfigNode.setStorageGroup(setStorageGroupStatement.getStorageGroupPath());
            if (setStorageGroupStatement.getTTL() != null) {
                localConfigNode.setTTL(
                        setStorageGroupStatement.getStorageGroupPath(), setStorageGroupStatement.getTTL());
            }
            // schemaReplicationFactor, dataReplicationFactor, timePartitionInterval are ignored
            future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
        } catch (Exception e) {
            LOGGER.error("Failed to set storage group, caused by ", e);
            future.setException(e);
        }
        return null;
    }

    @Override
    public SettableFuture<ConfigTaskResult> showStorageGroup(ShowStorageGroupStatement showStorageGroupStatement) {
        return null;
    }
}
