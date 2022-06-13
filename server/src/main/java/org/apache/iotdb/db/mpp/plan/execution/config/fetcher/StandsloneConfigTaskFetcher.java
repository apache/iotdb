package org.apache.iotdb.db.mpp.plan.execution.config.fetcher;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.db.localconfignode.LocalConfigNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.CountStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ShowStorageGroupTask;
import org.apache.iotdb.db.mpp.plan.execution.config.ShowTTLTask;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CountStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DeleteStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowStorageGroupStatement;
import org.apache.iotdb.db.mpp.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.mpp.plan.statement.sys.FlushStatement;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StandsloneConfigTaskFetcher implements IConfigTaskFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandsloneConfigTaskFetcher.class);

  private static final class StandsloneConfigTaskFetcherHolder {
    private static final StandsloneConfigTaskFetcher INSTANCE = new StandsloneConfigTaskFetcher();

    private StandsloneConfigTaskFetcherHolder() {}
  }

  public static StandsloneConfigTaskFetcher getInstance() {
    return StandsloneConfigTaskFetcher.StandsloneConfigTaskFetcherHolder.INSTANCE;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setStorageGroup(
      SetStorageGroupStatement setStorageGroupStatement) {
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
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showStorageGroup(
      ShowStorageGroupStatement showStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    Map<String, TStorageGroupSchema> storageGroupSchemaMap = new HashMap<>();
    try {
      LocalConfigNode localConfigNode = LocalConfigNode.getInstance();
      List<PartialPath> partialPaths =
          localConfigNode.getMatchedStorageGroups(
              showStorageGroupStatement.getPathPattern(), showStorageGroupStatement.isPrefixPath());
      for (PartialPath storageGroupPath : partialPaths) {
        IStorageGroupMNode storageGroupMNode =
            localConfigNode.getStorageGroupNodeByPath(storageGroupPath);
        String storageGroup = storageGroupMNode.getFullPath();
        TStorageGroupSchema storageGroupSchema = storageGroupMNode.getStorageGroupSchema();
        storageGroupSchemaMap.put(storageGroup, storageGroupSchema);
        // build TSBlock
        ShowStorageGroupTask.buildTSBlock(storageGroupSchemaMap, future);
      }
    } catch (MetadataException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> countStorageGroup(
      CountStorageGroupStatement countStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    int storageGroupNum;
    try {
      storageGroupNum =
          LocalConfigNode.getInstance()
              .getStorageGroupNum(
                  countStorageGroupStatement.getPartialPath(),
                  countStorageGroupStatement.isPrefixPath());
      // build TSBlock
      CountStorageGroupTask.buildTSBlock(storageGroupNum, future);
    } catch (MetadataException e) {
      future.setException(e);
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> createFunction(
      String udfName, String className, List<String> uris) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      UDFRegistrationService.getInstance()
          .register(udfName, className, uris, UDFExecutableManager.getInstance(), true);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format(
              "Failed to create function %s(%s), URI: %s, because %s.",
              udfName, className, uris, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new StatementExecutionException(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage(message)));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> deleteStorageGroup(
      DeleteStorageGroupStatement deleteStorageGroupStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      List<PartialPath> deletePathList =
          deleteStorageGroupStatement.getPrefixPath().stream()
              .map(
                  path -> {
                    try {
                      return new PartialPath(path);
                    } catch (IllegalPathException e) {
                      return null;
                    }
                  })
              .collect(Collectors.toList());
      LocalConfigNode.getInstance().deleteStorageGroups(deletePathList);
    } catch (MetadataException e) {
      future.setException(e);
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> dropFunction(String udfName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      UDFRegistrationService.getInstance().deregister(udfName);
      future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    } catch (Exception e) {
      final String message =
          String.format("Failed to drop function %s, because %s.", udfName, e.getMessage());
      LOGGER.error(message, e);
      future.setException(
          new StatementExecutionException(
              new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                  .setMessage(message)));
    }
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> setTTL(SetTTLStatement setTTLStatement, String taskName) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    try {
      LocalConfigNode.getInstance()
          .setTTL(setTTLStatement.getStorageGroupPath(), setTTLStatement.getTTL());
    } catch (MetadataException | IOException e) {
      future.setException(e);
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS));
    return future;
  }

  @Override
  public SettableFuture<ConfigTaskResult> flush(FlushStatement flushStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();

    return null;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showCluster(ShowClusterStatement showClusterStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    return null;
  }

  @Override
  public SettableFuture<ConfigTaskResult> showTTL(ShowTTLStatement showTTLStatement) {
    SettableFuture<ConfigTaskResult> future = SettableFuture.create();
    List<PartialPath> storageGroupPaths = showTTLStatement.getPaths();
    Map<String, Long> storageGroupToTTL = new HashMap<>();
    try {
      Map<PartialPath, Long> allStorageGroupToTTL =
          LocalConfigNode.getInstance().getStorageGroupsTTL();
      for (PartialPath storageGroupPath : storageGroupPaths) {
        if (showTTLStatement.isAll()) {
          storageGroupToTTL.put(
              storageGroupPath.getFullPath(), allStorageGroupToTTL.get(storageGroupPath));
        } else {
          List<PartialPath> matchedStorageGroupPaths =
              LocalConfigNode.getInstance()
                  .getMatchedStorageGroups(storageGroupPath, showTTLStatement.isPrefixPath());
          for (PartialPath matchedStorageGroupPath : matchedStorageGroupPaths) {
            storageGroupToTTL.put(
                matchedStorageGroupPath.getFullPath(),
                allStorageGroupToTTL.get(matchedStorageGroupPath));
          }
        }
      }
    } catch (MetadataException e) {
      future.setException(e);
    }
    // build TSBlock
    ShowTTLTask.buildTSBlock(storageGroupToTTL, future);
    return future;
  }
}
