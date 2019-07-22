package org.apache.iotdb.db.engine.merge.manage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * MergeContext records the shared information between merge sub-tasks.
 */
public class MergeContext {

  private Map<TsFileResource, Integer> mergedChunkCnt = new HashMap<>();
  private Map<TsFileResource, Integer> unmergedChunkCnt = new HashMap<>();
  private Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes = new HashMap<>();

  private int totalChunkWritten;

  public void clear() {
    mergedChunkCnt.clear();
    unmergedChunkCnt.clear();
    unmergedChunkStartTimes.clear();
  }

  public Map<TsFileResource, Integer> getMergedChunkCnt() {
    return mergedChunkCnt;
  }

  public void setMergedChunkCnt(
      Map<TsFileResource, Integer> mergedChunkCnt) {
    this.mergedChunkCnt = mergedChunkCnt;
  }

  public Map<TsFileResource, Integer> getUnmergedChunkCnt() {
    return unmergedChunkCnt;
  }

  public void setUnmergedChunkCnt(
      Map<TsFileResource, Integer> unmergedChunkCnt) {
    this.unmergedChunkCnt = unmergedChunkCnt;
  }

  public Map<TsFileResource, Map<Path, List<Long>>> getUnmergedChunkStartTimes() {
    return unmergedChunkStartTimes;
  }

  public void setUnmergedChunkStartTimes(
      Map<TsFileResource, Map<Path, List<Long>>> unmergedChunkStartTimes) {
    this.unmergedChunkStartTimes = unmergedChunkStartTimes;
  }

  public int getTotalChunkWritten() {
    return totalChunkWritten;
  }

  public void setTotalChunkWritten(int totalChunkWritten) {
    this.totalChunkWritten = totalChunkWritten;
  }
}
