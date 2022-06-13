package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import org.apache.iotdb.common.rpc.thrift.TFlushReq;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;

import com.google.common.util.concurrent.SettableFuture;

import java.util.List;

public interface IConfigTaskFetcher {

  SettableFuture<ConfigTaskResult> setStorageGroup(
      SetStorageGroupStatement setStorageGroupStatement);

  SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement);

  SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement);

  SettableFuture<ConfigTaskResult> createFunction(
      String udfName, String className, List<String> uris);

  SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement);

  SettableFuture<ConfigTaskResult> dropFunction(String udfName);

  SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName);

  SettableFuture<ConfigTaskResult> flush(TFlushReq tFlushReq);

  SettableFuture<ConfigTaskResult> showCluster();

  SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement);
}
