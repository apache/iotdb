package org.apache.iotdb.db.engine.merge;

import static org.apache.iotdb.db.engine.merge.sizeMerge.regularization.task.RegularizationMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.merge.sizeMerge.SizeMergeFileStrategy;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

public abstract class BaseMergeSeriesTask {

  protected MergeLogger mergeLogger;

  protected String taskName;
  protected MergeResource resource;
  protected MergeContext mergeContext;
  protected long timeBlock;
  protected List<Path> unmergedSeries;

  protected BaseMergeSeriesTask(MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.timeBlock = IoTDBDescriptor.getInstance().getConfig().getMergeFileTimeBlock();
    this.unmergedSeries = unmergedSeries;
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> getNewFileWriter(
      SizeMergeFileStrategy strategy,
      List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs, int idx)
      throws IOException {
    switch (strategy) {
      case REGULARIZATION:
        return getFileWriter(newTsFilePairs, idx);
      case INDEPENDENCE:
      default:
        if (newTsFilePairs == null) {
          newTsFilePairs = new ArrayList<>();
        }
        Pair<RestorableTsFileIOWriter, TsFileResource> pair = createNewFileWriter();
        newTsFilePairs.add(pair);
        return pair;
    }
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> createNewFileWriter()
      throws IOException {
    // use the minimum version as the version of the new file
    File newFile = createNewFile();
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
        new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
    mergeLogger.logNewFile(newTsFilePair.right);
    return newTsFilePair;
  }

  protected Pair<RestorableTsFileIOWriter, TsFileResource> getFileWriter(
      List<Pair<RestorableTsFileIOWriter, TsFileResource>> newTsFilePairs, int idx)
      throws IOException {
    if (newTsFilePairs == null) {
      newTsFilePairs = new ArrayList<>();
    }
    File newFile = createNewFile();
    if (idx < newTsFilePairs.size()) {
      return newTsFilePairs.get(idx);
    } else {
      Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
          new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
      mergeLogger.logNewFile(newTsFilePair.right);
      newTsFilePairs.add(newTsFilePair);
      return newTsFilePair;
    }
  }

  private File createNewFile() {
    long currFileVersion =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[1]);
    long prevMergeNum =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[2]);
    File parent = resource.getSeqFiles().get(0).getFile().getParentFile();
    File newFile = FSFactoryProducer.getFSFactory().getFile(parent,
        System.currentTimeMillis() + TSFILE_SEPARATOR + currFileVersion + TSFILE_SEPARATOR + (
            prevMergeNum + 1) + TSFILE_SUFFIX + MERGE_SUFFIX);
    return newFile;
  }
}
