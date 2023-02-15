package org.apache.iotdb.db.metadata.mtree.store.disk.memcontrol;

public interface IReleaseFlushStrategy {
  /** Check if exceed release threshold */
  boolean isExceedReleaseThreshold();

  /** Check if exceed flush threshold */
  boolean isExceedFlushThreshold();
}
