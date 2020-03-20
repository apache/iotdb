package org.apache.iotdb.db.engine.merge.inplace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.db.engine.merge.IMergeFileSelector;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.utils.MergeFileSeletorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InplaceMaxFileMergeFileSelector implements IMergeFileSelector {

  private static final Logger logger = LoggerFactory
      .getLogger(InplaceMaxFileMergeFileSelector.class);

  @Override
  public MergeResource selectMergedFiles(List<TsFileResource> seqFiles,
      List<TsFileResource> unseqfiles, long budget, long timeLowerBound) throws MergeException {
    MergeResource resource = new MergeResource();
    seqFiles = seqFiles.stream().filter(
        tsFileResource -> MergeFileSeletorUtils.filterResource(tsFileResource, timeLowerBound))
        .collect(Collectors.toList());
    List<TsFileResource> selectedSeqFiles = new ArrayList<>();
    List<TsFileResource> selectedUnseqFiles = new ArrayList<>();
    long startTime = System.currentTimeMillis();
    try {
      logger.info("Selecting merge candidates from {} seqFile, {} unseqFiles", seqFiles.size(),
          unseqfiles.size());
      select(false);
      if (selectedUnseqFiles.isEmpty()) {
        select(true);
      }
      if (selectedUnseqFiles.isEmpty()) {
        logger.info("No merge candidates are found");
        return resource;
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.info("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(), selectedUnseqFiles.size(), totalCost,
          System.currentTimeMillis() - startTime);
    }
    return resource;
  }
}
