package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;

public interface IConfigTaskFetcher {

    SettableFuture<ConfigTaskResult> setStorageGroup(SetStorageGroupStatement setStorageGroupStatement);

    SettableFuture<ConfigTaskResult> showStorageGroup(ShowStorageGroupStatement showStorageGroupStatement);
}
