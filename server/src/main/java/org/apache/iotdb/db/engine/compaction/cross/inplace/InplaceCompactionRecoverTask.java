package org.apache.iotdb.db.engine.compaction.cross.inplace;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.compaction.cross.AbstractCrossSpaceCompactionRecoverTask;
import org.apache.iotdb.db.engine.compaction.cross.inplace.task.RecoverCrossMergeTask;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InplaceCompactionRecoverTask extends InplaceCompactionTask {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(AbstractCrossSpaceCompactionRecoverTask.class);

  public InplaceCompactionRecoverTask(CompactionContext context) {
    super(context);
  }

  @Override
  public void doCompaction() throws IOException, MetadataException {
    String taskName = storageGroupName + "-" + System.currentTimeMillis();
    Iterator<TsFileResource> seqIterator = seqTsFileResourceList.iterator();
    Iterator<TsFileResource> unSeqIterator = unSeqTsFileResourceList.iterator();
    List<TsFileResource> seqFileList = new ArrayList<>();
    List<TsFileResource> unSeqFileList = new ArrayList<>();
    while (seqIterator.hasNext()) {
      seqFileList.add(seqIterator.next());
    }
    while (unSeqIterator.hasNext()) {
      unSeqFileList.add(unSeqIterator.next());
    }
    RecoverCrossMergeTask recoverCrossMergeTask =
        new RecoverCrossMergeTask(
            seqFileList,
            unSeqFileList,
            storageGroupDir,
            this::mergeEndAction,
            taskName,
            IoTDBDescriptor.getInstance().getConfig().isForceFullMerge(),
            storageGroupName);
    LOGGER.info("{} a RecoverMergeTask {} starts...", storageGroupName, taskName);
    recoverCrossMergeTask.recoverMerge(
        IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot());
    if (!IoTDBDescriptor.getInstance().getConfig().isContinueMergeAfterReboot()) {
      for (TsFileResource seqFile : seqFileList) {
        ModificationFile.getCompactionMods(seqFile).remove();
      }
    }
  }
}
