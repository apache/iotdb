package org.apache.iotdb.db.engine.merge.testMerge.level.task;

import static org.apache.iotdb.db.engine.merge.testMerge.level.task.LevelMergeTask.MERGE_SUFFIX;
import static org.apache.iotdb.db.utils.MergeUtils.writeBatchPoint;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SEPARATOR;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.TSFILE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.engine.merge.MergeLogger;
import org.apache.iotdb.db.engine.merge.manage.MergeContext;
import org.apache.iotdb.db.engine.merge.manage.MergeResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.db.utils.MergeUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LevelMergeSeriesTask {

  private static final Logger logger = LoggerFactory.getLogger(
      LevelMergeSeriesTask.class);
  protected MergeLogger mergeLogger;

  protected String taskName;
  protected MergeResource resource;
  protected MergeContext mergeContext;
  protected List<Path> unmergedSeries;
  protected List<List<Path>> devicePaths;
  protected int mergedSeriesCnt;
  private double progress;
  private final int mergeLevelMultiple;
  private final int chunkMergePointThreshold;//C0的chunk点多少

  private RestorableTsFileIOWriter newFileWriter;
  private TsFileResource newResource;

  public LevelMergeSeriesTask(
      MergeContext context, String taskName, MergeLogger mergeLogger,
      MergeResource mergeResource, List<Path> unmergedSeries) {
    this.mergeContext = context;
    this.taskName = taskName;
    this.mergeLogger = mergeLogger;
    this.resource = mergeResource;
    this.unmergedSeries = unmergedSeries;
    this.devicePaths = MergeUtils.splitPathsByDevice(unmergedSeries);
    this.mergeLevelMultiple = 10;
    this.chunkMergePointThreshold = 100;
  }

  public List<TsFileResource> mergeSeries() throws IOException {
    if (logger.isInfoEnabled()) {
      long totalChunkPoint = 0;
      long chunkNum = 0;
      for (TsFileResource seqFile : resource.getSeqFiles()) {
        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(seqFile);
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          chunkNum++;
          totalChunkPoint += chunkMetadata.getNumOfPoints();
        }
      }
      logger.info("merge before seqFile chunk large = {}", totalChunkPoint * 1.0 / chunkNum);
    }
    if (logger.isInfoEnabled()) {
      logger.info("{} starts to merge series", taskName);
    }
    long startTime = System.currentTimeMillis();
    Pair<RestorableTsFileIOWriter, TsFileResource> tsFilePair = createNewFileWriter();
    newFileWriter = tsFilePair.left;
    newResource = tsFilePair.right;
    mergePaths();
    List<TsFileResource> newResources = new ArrayList<>();
    newResources.add(newResource);
    newFileWriter.endFile();

    mergeLogger.logAllTsEnd();

    if (logger.isInfoEnabled()) {
      logger.info("{} all series are merged after {}ms", taskName,
          System.currentTimeMillis() - startTime);
    }
    if (logger.isInfoEnabled()) {
      long totalChunkPoint = 0;
      long chunkNum = 0;
      for (TsFileResource seqFile : newResources) {
        List<ChunkMetadata> chunkMetadataList = resource.queryChunkMetadata(seqFile);
        for (ChunkMetadata chunkMetadata : chunkMetadataList) {
          chunkNum++;
          totalChunkPoint += chunkMetadata.getNumOfPoints();
        }
      }
      logger.info("merge after seqFile chunk large = {}", totalChunkPoint * 1.0 / chunkNum);
    }

    for (TsFileResource tsFileResource : newResources) {
      File oldTsFile = tsFileResource.getFile();
      File newTsFile = new File(oldTsFile.getParent(),
          oldTsFile.getName().replace(MERGE_SUFFIX, ""));
      oldTsFile.renameTo(newTsFile);
      tsFileResource.setFile(newTsFile);
      tsFileResource.serialize();
    }
    return newResources;
  }

  private void mergePaths() throws IOException {
    for (List<Path> pathList : devicePaths) {
      newFileWriter.startChunkGroup(pathList.get(0).getDevice());
      for (Path path : pathList) {
        long mergeNum = 0;
        long levelChunkPointThreshold = this.chunkMergePointThreshold;
        IChunkWriter chunkWriter = resource.getChunkWriter(path);
        newFileWriter.addSchema(path, chunkWriter.getMeasurementSchema());
        QueryContext context = new QueryContext();
        IBatchReader tsFilesReader = new SeriesRawDataBatchReader(path,
            chunkWriter.getMeasurementSchema().getType(),
            context, resource.getSeqFiles(), resource.getUnseqFiles(), null, null);
        while (tsFilesReader.hasNextBatch()) {
          BatchData batchData = tsFilesReader.nextBatch();
          newResource.updateStartTime(path.getDevice(), batchData.getTimeByIndex(0));
          for (int i = 0; i < batchData.length(); i++) {
            writeBatchPoint(batchData, i, chunkWriter);
            if (chunkWriter.getPtNum() >= levelChunkPointThreshold) {
              mergeNum++;
              levelChunkPointThreshold =
                  (long) (this.chunkMergePointThreshold * Math.pow(mergeLevelMultiple, mergeNum));
              mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
              chunkWriter.writeToFileWriter(newFileWriter);
              chunkWriter = new ChunkWriterImpl(chunkWriter.getMeasurementSchema());
              resource.putChunkWriter(path, chunkWriter);
            }
          }
          if (!tsFilesReader.hasNextBatch()) {
            newResource.updateEndTime(path.getDevice(),
                batchData.getTimeByIndex(batchData.length() - 1));
          }
        }
        chunkWriter.writeToFileWriter(newFileWriter);
        mergeContext.incTotalPointWritten(chunkWriter.getPtNum());
        tsFilesReader.close();
      }
      newFileWriter.writeVersion(0L);
      newFileWriter.endChunkGroup();
      mergedSeriesCnt += pathList.size();
      logMergeProgress();
    }
  }

  private Pair<RestorableTsFileIOWriter, TsFileResource> createNewFileWriter()
      throws IOException {
    // use the minimum version as the version of the new file
    File newFile = createNewFile();
    Pair<RestorableTsFileIOWriter, TsFileResource> newTsFilePair = new Pair<>(
        new RestorableTsFileIOWriter(newFile), new TsFileResource(newFile));
    mergeLogger.logNewFile(newTsFilePair.right);
    return newTsFilePair;
  }

  private File createNewFile() {
    long currFileVersion =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[1]);
    long prevMergeVersion =
        Long.parseLong(
            resource.getSeqFiles().get(0).getFile().getName().replace(TSFILE_SUFFIX, "")
                .split(TSFILE_SEPARATOR)[2]);
    File parent = resource.getSeqFiles().get(0).getFile().getParentFile();
    return FSFactoryProducer.getFSFactory().getFile(parent,
        System.currentTimeMillis() + TSFILE_SEPARATOR + currFileVersion + TSFILE_SEPARATOR
            + (prevMergeVersion + 1) + TSFILE_SUFFIX + MERGE_SUFFIX);
  }

  private void logMergeProgress() {
    if (logger.isInfoEnabled()) {
      double newProgress = 100 * mergedSeriesCnt / (double) (unmergedSeries.size());
      if (newProgress - progress >= 1.0) {
        progress = newProgress;
        logger.info("{} has merged {}% series", taskName, progress);
      }
    }
  }

}
