package org.apache.iotdb.db.engine.compaction.inner;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InnerSeqCompactionTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device0.sensor1",
        COMPACTION_TEST_SG + ".device0.sensor2"
      };

  @Before
  public void setUp() throws MetadataException {
    IoTDB.metaManager.init();
    IoTDB.metaManager.setStorageGroup(new PartialPath(COMPACTION_TEST_SG));
    for (String fullPath : fullPaths) {
      PartialPath path = new PartialPath(fullPath);
      IoTDB.metaManager.createTimeseries(
          path,
          TSDataType.INT64,
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
          TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
    }
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    CompactionClearUtils.clearAllCompactionFiles();
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    IoTDB.metaManager.clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testDeserializePage() throws IllegalPathException, IOException {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(100000);
    // merge 2 files
    Set<String> fullPath1 = new HashSet<>();
    fullPath1.add(fullPaths[0]);
    fullPath1.add(fullPaths[1]);
    fullPath1.add(fullPaths[2]);
    List<List<Long>> chunkPagePointsNum1 = new ArrayList<>();
    List<Long> pagePointsNum = new ArrayList<>();
    pagePointsNum.add(100L);
    chunkPagePointsNum1.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(200L);
    chunkPagePointsNum1.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(300L);
    chunkPagePointsNum1.add(pagePointsNum);
    TsFileResource tsFileResource1 = CompactionFileGeneratorUtils.generateTsFileResource(true, 1);
    TsFileResource targetTsFileResource =
        CompactionFileGeneratorUtils.getTargetTsFileResourceFromSourceResource(tsFileResource1);
    CompactionFileGeneratorUtils.generateNewTsFile(
        fullPath1, chunkPagePointsNum1, 0, tsFileResource1);
    Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
    toDeleteTimeseriesAndTime.put(fullPaths[2], new Pair<>(250L, 300L));
    CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, tsFileResource1, false);

    Set<String> fullPath2 = new HashSet<>();
    fullPath2.add(fullPaths[0]);
    fullPath2.add(fullPaths[1]);
    fullPath2.add(fullPaths[2]);
    List<List<Long>> chunkPagePointsNum2 = new ArrayList<>();
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(100L);
    chunkPagePointsNum2.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(200L);
    chunkPagePointsNum2.add(pagePointsNum);
    pagePointsNum = new ArrayList<>();
    pagePointsNum.add(300L);
    chunkPagePointsNum2.add(pagePointsNum);
    TsFileResource tsFileResource2 = CompactionFileGeneratorUtils.generateTsFileResource(true, 2);
    CompactionFileGeneratorUtils.generateNewTsFile(
        fullPath2, chunkPagePointsNum2, 601, tsFileResource2);
    List<TsFileResource> toMergeResources = new ArrayList<>();
    toMergeResources.add(tsFileResource1);
    toMergeResources.add(tsFileResource2);
    toDeleteTimeseriesAndTime = new HashMap<>();
    toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(250L, 300L));
    CompactionFileGeneratorUtils.generateMods(toDeleteTimeseriesAndTime, tsFileResource1, true);
    Map<String, List<TimeValuePair>> sourceData =
        CompactionCheckerUtils.readFiles(toMergeResources);
    List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
    timeValuePairs.removeIf(
        timeValuePair ->
            timeValuePair.getTimestamp() >= 250L && timeValuePair.getTimestamp() <= 300L);
    CompactionLogger compactionLogger = new CompactionLogger("target", COMPACTION_TEST_SG);
    CompactionUtils.compact(
        targetTsFileResource,
        toMergeResources,
        COMPACTION_TEST_SG,
        compactionLogger,
        new HashSet<>(),
        true);
    SizeTiredCompactionTask.renameLevelFilesMods(toMergeResources, targetTsFileResource);
    CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResource);
    List<List<Long>> chunkPagePointsNumMerged = new ArrayList<>();
    CompactionCheckerUtils.checkChunkAndPage(targetTsFileResource);

    // merge 3 files

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
  }

  @Test
  public void testAppendPage() {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(1);
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(100000);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }

  @Test
  public void testAppendChunk() {
    int prevMergePagePointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergePagePointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergePagePointNumberThreshold(100000);
    int prevMergeChunkPointNumberThreshold =
        IoTDBDescriptor.getInstance().getConfig().getMergeChunkPointNumberThreshold();
    IoTDBDescriptor.getInstance().getConfig().setMergeChunkPointNumberThreshold(100000);

    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergePagePointNumberThreshold(prevMergePagePointNumberThreshold);
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setMergeChunkPointNumberThreshold(prevMergeChunkPointNumberThreshold);
  }
}
