package org.apache.iotdb.db.storageengine.dataregion.compaction.tool.reader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.UnseqSpaceStatistics;

import java.util.concurrent.Callable;

public class SingleSequenceFileTask implements Callable<TaskSummary> {
  private UnseqSpaceStatistics unseqSpaceStatistics;
  private String seqFile;

  public SingleSequenceFileTask(UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {
    this.unseqSpaceStatistics = unseqSpaceStatistics;
    this.seqFile = seqFile;
  }

  @Override
  public TaskSummary call() throws Exception {
    return checkSeqFile(unseqSpaceStatistics, seqFile);
  }

  private TaskSummary checkSeqFile(UnseqSpaceStatistics unseqSpaceStatistics, String seqFile) {

    return new TaskSummary();
  }
}
