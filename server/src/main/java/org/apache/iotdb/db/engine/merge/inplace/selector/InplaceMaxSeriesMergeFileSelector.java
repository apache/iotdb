package org.apache.iotdb.db.engine.merge.inplace.selector;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSelectorUtils;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;

public class InplaceMaxSeriesMergeFileSelector implements IMergeFileSelector {

  public static final int MAX_SERIES_NUM = 1024;

  private InplaceMaxFileSelector maxFileMergeFileSelector;
  private SelectorContext selectorContext;

  @Override
  public Pair<MergeResource, SelectorContext> selectMergedFiles() throws MergeException {
    return MergeFileSelectorUtils
        .selectSeries(maxFileMergeFileSelector.getSeqFiles(),
            maxFileMergeFileSelector.getUnseqFiles(), this::searchMaxSeriesNum,
            this.selectorContext);
  }

  public InplaceMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget) {
    this(seqFiles, unseqFiles, budget, Long.MIN_VALUE);
  }

  public InplaceMaxSeriesMergeFileSelector(Collection<TsFileResource> seqFiles,
      Collection<TsFileResource> unseqFiles, long budget, long timeLowerBound) {
    this.selectorContext = new SelectorContext();
    this.maxFileMergeFileSelector = new InplaceMaxFileSelector(seqFiles, unseqFiles,
        budget, timeLowerBound);
  }

  private Pair<List<TsFileResource>, List<TsFileResource>> searchMaxSeriesNum() throws IOException {
    return binSearch();
  }

  private Pair<List<TsFileResource>, List<TsFileResource>> binSearch() throws IOException {
    List<TsFileResource> lastSelectedSeqFiles = Collections.emptyList();
    List<TsFileResource> lastSelectedUnseqFiles = Collections.emptyList();
    long lastTotalMemoryCost = 0;
    int lb = 0;
    int ub = MAX_SERIES_NUM + 1;
    while (true) {
      int mid = (lb + ub) / 2;
      if (mid == lb) {
        break;
      }
      selectorContext.setConcurrentMergeNum(mid);
      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = maxFileMergeFileSelector
          .select(false);
      if (selectedFiles.right.isEmpty()) {
        selectedFiles = maxFileMergeFileSelector.select(true);
      }
      if (selectedFiles.right.isEmpty()) {
        ub = mid;
      } else {
        lastSelectedSeqFiles = selectedFiles.left;
        lastSelectedUnseqFiles = selectedFiles.right;
        lastTotalMemoryCost = maxFileMergeFileSelector.getTotalCost();
        lb = mid;
      }
    }
    selectorContext.setConcurrentMergeNum(lb);
    selectorContext.setTotalCost(lastTotalMemoryCost);
    return new Pair<>(lastSelectedSeqFiles, lastSelectedUnseqFiles);
  }
}
