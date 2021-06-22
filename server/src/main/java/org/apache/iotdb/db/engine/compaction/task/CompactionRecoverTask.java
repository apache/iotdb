package org.apache.iotdb.db.engine.compaction.task;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.DirectoryManager;
import org.apache.iotdb.db.engine.compaction.CompactionScheduler;
import org.apache.iotdb.db.engine.compaction.cross.inplace.recover.MergeLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor.CompactionRecoverCallBack;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceManager;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class CompactionRecoverTask implements Callable<Void> {
  private CompactionRecoverCallBack compactionRecoverCallBack;
  private TsFileResourceManager tsFileResourceManager;
  private String logicalStorageGroupName;
  private String virtualStorageGroupId;

  public CompactionRecoverTask(
      CompactionRecoverCallBack compactionRecoverCallBack,
      TsFileResourceManager tsFileResourceManager,
      String logicalStorageGroupName,
      String virtualStorageGroupId) {
    this.compactionRecoverCallBack = compactionRecoverCallBack;
    this.tsFileResourceManager = tsFileResourceManager;
    this.logicalStorageGroupName = logicalStorageGroupName;
    this.virtualStorageGroupId = virtualStorageGroupId;
  }

  @Override
  public Void call() throws Exception {
    recoverInnerCompaction(true);
    recoverInnerCompaction(false);
    recoverCrossCompaction();
    CompactionScheduler.decPartitionCompaction(
        logicalStorageGroupName + "-" + logicalStorageGroupName, 0);
    compactionRecoverCallBack.call();
    return null;
  }

  private void recoverInnerCompaction(boolean isSequence) throws Exception {
    Set<Long> timePartitions = tsFileResourceManager.getTimePartitions();
    List<String> dirs;
    if (isSequence) {
      dirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    } else {
      dirs = DirectoryManager.getInstance().getAllUnSequenceFileFolders();
    }
    for (String dir : dirs) {
      String storageGroupDir =
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition;
        File[] compactionLogs = CompactionUtils.findInnerSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getInnerCompactionStrategy()
              .getCompactionRecoverTask(
                  tsFileResourceManager.getStorageGroupName(),
                  tsFileResourceManager.getVirtualStorageGroup(),
                  timePartition,
                  compactionLog,
                  storageGroupDir,
                  isSequence
                      ? tsFileResourceManager.getSequenceListByTimePartition(timePartition)
                      : tsFileResourceManager.getUnsequenceListByTimePartition(timePartition),
                  isSequence
                      ? tsFileResourceManager.getSequenceRecoverTsFileResources()
                      : tsFileResourceManager.getUnsequenceRecoverTsFileResources(),
                  isSequence)
              .call();
        }
      }
    }
  }

  private void recoverCrossCompaction() throws Exception {
    Set<Long> timePartitions = tsFileResourceManager.getTimePartitions();
    List<String> sequenceDirs = DirectoryManager.getInstance().getAllSequenceFileFolders();
    for (String dir : sequenceDirs) {
      String storageGroupDir =
          dir + File.separator + logicalStorageGroupName + File.separator + virtualStorageGroupId;
      for (Long timePartition : timePartitions) {
        String timePartitionDir = storageGroupDir + File.separator + timePartition;
        File[] compactionLogs = MergeLogger.findCrossSpaceCompactionLogs(timePartitionDir);
        for (File compactionLog : compactionLogs) {
          CompactionContext context = new CompactionContext();
          context.setCompactionLogFile(compactionLog);
          context.setSequenceFileResourceList(
              tsFileResourceManager.getSequenceListByTimePartition(timePartition));
          context.setUnsequenceFileResourceList(
              tsFileResourceManager.getUnsequenceListByTimePartition(timePartition));
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getCrossCompactionStrategy()
              .getCompactionRecoverTask(context)
              .call();
        }
      }
    }
  }
}
