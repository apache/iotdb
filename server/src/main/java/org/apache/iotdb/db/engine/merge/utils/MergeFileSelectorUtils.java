package org.apache.iotdb.db.engine.merge.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.iotdb.db.engine.merge.IFileSelector;
import org.apache.iotdb.db.engine.merge.IFileSeriesSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.apache.iotdb.db.utils.UpgradeUtils;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeFileSelectorUtils {

  private static final Logger logger = LoggerFactory.getLogger(MergeFileSelectorUtils.class);

  public static boolean filterResource(TsFileResource res, long timeLowerBound) {
    return res.isClosed() && !res.isDeleted() && res.stillLives(timeLowerBound);
  }

  public static boolean checkForUpgrade(TsFileResource unseqFile,
      Set<Integer> tmpSelectedSeqFiles, List<TsFileResource> seqFiles) {
    // reject the selection if it contains files that should be upgraded
    boolean isNeedUpgrade = false;
    if (UpgradeUtils.isNeedUpgrade(unseqFile)) {
      isNeedUpgrade = true;
    }
    for (Integer seqFileIdx : tmpSelectedSeqFiles) {
      if (UpgradeUtils.isNeedUpgrade(seqFiles.get(seqFileIdx))) {
        isNeedUpgrade = true;
        break;
      }
    }
    return isNeedUpgrade;
  }

  public static Pair<MergeResource, SelectorContext> select(MergeResource resource,
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      IFileSelector selector, SelectorContext selectorContext) throws MergeException {
    selectorContext.setStartTime(System.currentTimeMillis());
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqFiles.size());
      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = selector.select(false);
      if (selectedFiles.right.isEmpty()) {
        selectedFiles = selector.select(true);
      }
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
              + "time consumption {}ms",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size(),
          selectorContext.getTotalCost(),
          System.currentTimeMillis() - selectorContext.getStartTime());
    }
    return new Pair<>(resource, selectorContext);
  }

  public static Pair<MergeResource, SelectorContext> selectSeries(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles,
      IFileSeriesSelector selector,
      SelectorContext selectorContext) throws MergeException {
    selectorContext.setStartTime(System.currentTimeMillis());
    MergeResource resource = new MergeResource();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqFiles.size());

      Pair<List<TsFileResource>, List<TsFileResource>> selectedFiles = selector
          .searchMaxSeriesNum();
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
}
