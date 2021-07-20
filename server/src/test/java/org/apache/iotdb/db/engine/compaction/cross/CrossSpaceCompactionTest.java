package org.apache.iotdb.db.engine.compaction.cross;

import static org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils.putOnePageChunk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.db.engine.compaction.inner.sizetired.SizeTiredCompactionTask;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionLogger;
import org.apache.iotdb.db.engine.compaction.inner.utils.CompactionUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionCheckerUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionClearUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionFileGeneratorUtils;
import org.apache.iotdb.db.engine.compaction.utils.CompactionTimeseriesType;
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

public class CrossSpaceCompactionTest {
  static final String COMPACTION_TEST_SG = "root.compactionTest";
  static final String[] fullPaths =
      new String[] {
        COMPACTION_TEST_SG + ".device0.sensor0",
        COMPACTION_TEST_SG + ".device0.sensor1",
        COMPACTION_TEST_SG + ".device0.sensor2",
        COMPACTION_TEST_SG + ".device0.sensor3",
        COMPACTION_TEST_SG + ".device0.sensor4",
        COMPACTION_TEST_SG + ".device0.sensor5",
        COMPACTION_TEST_SG + ".device0.sensor6",
        COMPACTION_TEST_SG + ".device0.sensor7",
        COMPACTION_TEST_SG + ".device0.sensor8",
        COMPACTION_TEST_SG + ".device0.sensor9",
        COMPACTION_TEST_SG + ".device1.sensor0",
        COMPACTION_TEST_SG + ".device1.sensor1",
        COMPACTION_TEST_SG + ".device1.sensor2",
        COMPACTION_TEST_SG + ".device1.sensor3",
        COMPACTION_TEST_SG + ".device1.sensor4",
      };
  static final CompactionTimeseriesType[] compactionTimeseriesTypes =
      new CompactionTimeseriesType[] {
        CompactionTimeseriesType.ALL_SAME,
        CompactionTimeseriesType.PART_SAME,
        CompactionTimeseriesType.NO_SAME
      };
  static final boolean[] compactionBeforeHasMods = new boolean[] {true, false};
  static final boolean[] compactionHasMods = new boolean[] {true, false};

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

  // unseq space only do deserialize page
  @Test
  public void test() throws IllegalPathException, IOException {
    for (CompactionTimeseriesType compactionTimeseriesType : compactionTimeseriesTypes) {
      for (boolean compactionBeforeHasMod : compactionBeforeHasMods) {
        for (boolean compactionHasMod : compactionHasMods) {
          List<TsFileResource> toMergeResources = new ArrayList<>();

          for (int i = 0; i < 6; i++) {
            Set<String> fullPath = new HashSet<>();
            if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
              fullPath.add(fullPaths[0]);
              fullPath.add(fullPaths[1]);
              fullPath.add(fullPaths[2]);
            } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
              if (i == 0) {
                fullPath.add(fullPaths[0]);
                fullPath.add(fullPaths[1]);
                fullPath.add(fullPaths[2]);
              } else if (i == 1) {
                fullPath.add(fullPaths[1]);
                fullPath.add(fullPaths[2]);
                fullPath.add(fullPaths[3]);
              } else {
                fullPath.add(fullPaths[2]);
                fullPath.add(fullPaths[3]);
                fullPath.add(fullPaths[4]);
              }
            } else {
              if (i == 0) {
                fullPath.add(fullPaths[0]);
                fullPath.add(fullPaths[1]);
                fullPath.add(fullPaths[2]);
              } else if (i == 1) {
                fullPath.add(fullPaths[3]);
                fullPath.add(fullPaths[4]);
                fullPath.add(fullPaths[5]);
              } else {
                fullPath.add(fullPaths[6]);
                fullPath.add(fullPaths[7]);
                fullPath.add(fullPaths[8]);
              }
            }
            List<List<long[][]>> chunkPagePointsRange;
            List<long[][]> pagePointsRange;
            TsFileResource tsFileResource = null;
            chunkPagePointsRange = new ArrayList<>();
            pagePointsRange = new ArrayList<>();
            pagePointsRange.add(new long[][] {{0L, 100L}});
            pagePointsRange.add(new long[][] {{100L, 300L}});
            pagePointsRange.add(new long[][] {{300L, 600L}});
            chunkPagePointsRange.add(pagePointsRange);
            pagePointsRange = new ArrayList<>();
            pagePointsRange.add(new long[][] {{0L, 100L}});
            pagePointsRange.add(new long[][] {{100L, 300L}});
            pagePointsRange.add(new long[][] {{300L, 600L}});
            chunkPagePointsRange.add(pagePointsRange);
            pagePointsRange = new ArrayList<>();
            pagePointsRange.add(new long[][] {{0L, 100L}});
            pagePointsRange.add(new long[][] {{100L, 300L}});
            pagePointsRange.add(new long[][] {{300L, 600L}});
            chunkPagePointsRange.add(pagePointsRange);
            fullPath.add(fullPaths[10]);
            pagePointsRange = new ArrayList<>();
            pagePointsRange.add(new long[][] {{0L, 1000L}});
            chunkPagePointsRange.add(pagePointsRange);
            tsFileResource = CompactionFileGeneratorUtils.generateTsFileResource(false, i + 1);
            CompactionFileGeneratorUtils.writeChunkToTsFileWithTimeRange(
                fullPath, chunkPagePointsRange, tsFileResource);
            toMergeResources.add(tsFileResource);
            // has mods files before compaction
            if (compactionBeforeHasMod) {
              Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
              if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
                toDeleteTimeseriesAndTime.put(
                    fullPaths[i], new Pair<>(i * 600L + 250L, i * 600L + 300L));
              } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
                if (i == 0) {
                  toDeleteTimeseriesAndTime.put(fullPaths[0], new Pair<>(250L, 300L));
                } else if (i == 1) {
                  toDeleteTimeseriesAndTime.put(
                      fullPaths[3], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                } else {
                  toDeleteTimeseriesAndTime.put(
                      fullPaths[4], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                }
              } else {
                if (i == 0) {
                  toDeleteTimeseriesAndTime.put(fullPaths[2], new Pair<>(250L, 300L));
                } else if (i == 1) {
                  toDeleteTimeseriesAndTime.put(
                      fullPaths[5], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                } else {
                  toDeleteTimeseriesAndTime.put(
                      fullPaths[8], new Pair<>(i * 600L + 250L, i * 600L + 300L));
                }
              }
              CompactionFileGeneratorUtils.generateMods(
                  toDeleteTimeseriesAndTime, tsFileResource, false);
            }
          }
          TsFileResource targetTsFileResource =
              CompactionFileGeneratorUtils.getTargetTsFileResourceFromSourceResource(
                  toMergeResources.get(0));
          Map<String, List<TimeValuePair>> sourceData =
              CompactionCheckerUtils.readFiles(toMergeResources);
          if (compactionHasMod) {
            Map<String, Pair<Long, Long>> toDeleteTimeseriesAndTime = new HashMap<>();
            toDeleteTimeseriesAndTime.put(fullPaths[1], new Pair<>(250L, 300L));
            CompactionFileGeneratorUtils.generateMods(
                toDeleteTimeseriesAndTime, toMergeResources.get(0), true);

            // remove data in source data list
            List<TimeValuePair> timeValuePairs = sourceData.get(fullPaths[1]);
            timeValuePairs.removeIf(
                timeValuePair ->
                    timeValuePair.getTimestamp() >= 250L && timeValuePair.getTimestamp() <= 300L);
          }
          CompactionLogger compactionLogger = new CompactionLogger("target", COMPACTION_TEST_SG);
          CompactionUtils.compact(
              targetTsFileResource,
              toMergeResources,
              COMPACTION_TEST_SG,
              compactionLogger,
              new HashSet<>(),
              false);
          SizeTiredCompactionTask.renameLevelFilesMods(toMergeResources, targetTsFileResource);
          CompactionCheckerUtils.checkDataAndResource(sourceData, targetTsFileResource);
          Map<String, List<List<Long>>> chunkPagePointsNumMerged = new HashMap<>();
          if (compactionTimeseriesType == CompactionTimeseriesType.ALL_SAME) {
            if (compactionBeforeHasMod) {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            } else {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            }
          } else if (compactionTimeseriesType == CompactionTimeseriesType.PART_SAME) {
            if (compactionBeforeHasMod) {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            } else {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            }
          } else {
            if (compactionBeforeHasMod) {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1149L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            } else {
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[0], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[1], 1200L);
              putOnePageChunk(chunkPagePointsNumMerged, fullPaths[2], 1200L);
            }
          }
          CompactionCheckerUtils.checkChunkAndPage(chunkPagePointsNumMerged, targetTsFileResource);
          CompactionClearUtils.clearAllCompactionFiles();
        }
      }
    }
  }
}
