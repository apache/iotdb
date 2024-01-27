package org.apache.iotdb.db.storageengine.dataregion.compaction.selector;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.AbstractCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

public interface ISettleSelector extends ICompactionSelector{
    @Override
    List<AbstractCompactionTask> selectSettleTask(List<TsFileResource> seqFiles, List<TsFileResource> unseqFiles);
}
