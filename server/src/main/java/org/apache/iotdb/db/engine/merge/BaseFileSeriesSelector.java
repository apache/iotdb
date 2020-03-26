package org.apache.iotdb.db.engine.merge;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeMemCalculator;
import org.apache.iotdb.db.engine.merge.utils.SelectorContext;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseFileSeriesSelector implements IMergeFileSelector {

  private static final Logger logger = LoggerFactory.getLogger(BaseFileSelector.class);
  public static final int MAX_SERIES_NUM = 256;

  protected SelectorContext selectorContext;
  protected MergeResource resource;

  protected BaseFileSelector maxFileMergeFileSelector;

  public Pair<MergeResource, SelectorContext> select() throws MergeException {
    selectorContext.setStartTime(System.currentTimeMillis());
    MergeResource resource = new MergeResource();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles",
          maxFileMergeFileSelector.seqFiles.size(),
          maxFileMergeFileSelector.unseqFiles.size());

      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = searchMaxSeriesNum();
      resource.setSeqFiles(selectedFiles.left);
      resource.setUnseqFiles(selectedFiles.right);
      resource.removeOutdatedSeqReaders();
      if (resource.getUnseqFiles().isEmpty()) {
        logger.info("No merge candidates are found");
        return new Pair<>(resource, selectorContext);
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "concurrent merge num {}" + "time consumption {}ms",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size(),
          selectorContext.getTotalCost(),
          selectorContext.getConcurrentMergeNum(),
          System.currentTimeMillis() - selectorContext.getStartTime());
    }
    return new Pair<>(resource, selectorContext);
  }

  protected Pair<List<TsFileResource>, List<TsFileResource>> searchMaxSeriesNum()
      throws IOException {
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
