package org.apache.iotdb.db.engine.compaction.execute.performer.impl;

import org.apache.iotdb.db.engine.compaction.execute.task.CompactionTaskSummary;

public class FastCompactionTaskSummary extends CompactionTaskSummary {
  public int CHUNK_NONE_OVERLAP;
  public int CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
  public int CHUNK_OVERLAP_OR_MODIFIED;

  public int PAGE_NONE_OVERLAP;
  public int PAGE_OVERLAP_OR_MODIFIED;
  public int PAGE_FAKE_OVERLAP;
  public int PAGE_NONE_OVERLAP_BUT_DESERIALIZE;

  public void increase(FastCompactionTaskSummary summary) {
    this.CHUNK_NONE_OVERLAP += summary.CHUNK_NONE_OVERLAP;
    this.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE += summary.CHUNK_NONE_OVERLAP_BUT_DESERIALIZE;
    this.CHUNK_OVERLAP_OR_MODIFIED += summary.CHUNK_OVERLAP_OR_MODIFIED;
    this.PAGE_NONE_OVERLAP += summary.PAGE_NONE_OVERLAP;
    this.PAGE_OVERLAP_OR_MODIFIED += summary.PAGE_OVERLAP_OR_MODIFIED;
    this.PAGE_FAKE_OVERLAP += summary.PAGE_FAKE_OVERLAP;
    this.PAGE_NONE_OVERLAP_BUT_DESERIALIZE += summary.PAGE_NONE_OVERLAP_BUT_DESERIALIZE;
  }
}
