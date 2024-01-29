package org.apache.iotdb.db.storageengine.dataregion.compaction.settle;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.SettleCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.dataregion.compaction.utils.TsFileGeneratorUtils.createTimeseries;

public class SettleCompactionTaskTest extends AbstractCompactionTest {

  @Test
  public void settleWithOnlyAllDirtyFiles()
      throws MetadataException, IOException, WriteProcessException {
    createFiles(6, 5, 10, 100, 0, 0, 0, 0, false, true);
    createFiles(5, 2, 3, 50, 0, 10000, 50, 50, false, false);

    generateModsFile(6, 6, seqResources, Long.MIN_VALUE, Long.MAX_VALUE);
    generateModsFile(6, 6, unseqResources, Long.MIN_VALUE, Long.MAX_VALUE);

    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    Map<PartialPath, List<TimeValuePair>> sourceDatas =
        readSourceFiles(createTimeseries(6, 6, false), Collections.emptyList());

    List<TsFileResource> allDeletedFiles = new ArrayList<>(seqResources);
    allDeletedFiles.addAll(unseqResources);

    SettleCompactionTask task =
        new SettleCompactionTask(
            0,
            tsFileManager,
            allDeletedFiles,
            Collections.emptyList(),
            new FastCompactionPerformer(false),
            0);
    Assert.assertTrue(task.start());

    for (TsFileResource tsFileResource : seqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      Assert.assertEquals(TsFileResourceStatus.DELETED, tsFileResource.getStatus());
    }

    Assert.assertEquals(0, tsFileManager.getTsFileList(true));
    Assert.assertEquals(0, tsFileManager.getTsFileList(false));

    validateTargetDatas(sourceDatas, Collections.emptyList());
  }

  @Test
  public void settleWithOnlyPartialDirtyFiles() {}
}
