package org.apache.iotdb.db.engine.merge.selection;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.MergeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSelectionUtils {

  private static final Logger logger = LoggerFactory.getLogger(FileSelectionUtils.class);

  public static void select(MergeResource resource, List<TsFileResource> selectedUnseqFiles,
      List<TsFileResource> selectedSeqFiles, long totalCost, Consumer<Boolean> select)
      throws MergeException {
    long startTime = System.currentTimeMillis();
    try {
      logger.debug("Selecting merge candidates from {} seqFile, {} unseqFiles",
          resource.getSeqFiles().size(), resource.getUnseqFiles().size());
      select.accept(false);
      if (selectedUnseqFiles.isEmpty()) {
        select.accept(true);
      }
      resource.setSeqFiles(selectedSeqFiles);
      resource.setUnseqFiles(selectedUnseqFiles);
      resource.removeOutdatedSeqReaders();
      if (selectedUnseqFiles.isEmpty() && selectedSeqFiles.isEmpty()) {
        logger.debug("No merge candidates are found");
        return;
      }
    } catch (IOException e) {
      throw new MergeException(e);
    }
    if (logger.isInfoEnabled()) {
      logger.debug("Selected merge candidates, {} seqFiles, {} unseqFiles, total memory cost {}, "
              + "time consumption {}ms",
          selectedSeqFiles.size(), selectedUnseqFiles.size(), totalCost,
          System.currentTimeMillis() - startTime);
    }
  }
}
