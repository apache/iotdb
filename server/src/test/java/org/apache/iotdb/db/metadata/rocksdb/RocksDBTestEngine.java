package org.apache.iotdb.db.metadata.rocksdb;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogWriter;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.utils.FileUtils;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.db.metadata.rocksdb.RocksDBReadWriteHandler.ROCKSDB_PATH;

public class RocksDBTestEngine {
  private static final Logger logger = LoggerFactory.getLogger(MManager.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private static final int BIN_CAPACITY = 100 * 1000;

  private MLogWriter logWriter;
  private File logFile;
  public static List<List<CreateTimeSeriesPlan>> timeSeriesSet = new ArrayList<>();
  public static Set<String> measurementPathSet = new HashSet<>();
  public static Set<String> innerPathSet = new HashSet<>();
  public static List<SetStorageGroupPlan> storageGroups = new ArrayList<>();

  public RocksDBTestEngine() {
    String schemaDir = config.getSchemaDir();
    String logFilePath = schemaDir + File.separator + MetadataConstant.METADATA_LOG;
    logFile = SystemFileFactory.INSTANCE.getFile(logFilePath);
  }

  @Test
  public void startTest() {
    RocksDBTestUtils.printMemInfo("Benchmark rocksdb start");
    try {
      prepareBenchmark();
      RocksDBTestUtils.printBenchmarkBaseline(
          storageGroups, timeSeriesSet, measurementPathSet, innerPathSet);
      /** rocksdb benchmark * */
      MRocksDBManager rocksDBManager = new MRocksDBManager();
      MRocksDBTest mRocksDBTest = new MRocksDBTest(rocksDBManager);
      mRocksDBTest.testStorageGroupCreation(storageGroups);
      mRocksDBTest.testTimeSeriesCreation(timeSeriesSet);
      RocksDBTestUtils.printReport(mRocksDBTest.benchmarkResults, "rocksDB");
      RocksDBTestUtils.printMemInfo("Benchmark finished");
    } catch (IOException | MetadataException e) {
      logger.error("Error happened when run benchmark", e);
    }
  }

  public void prepareBenchmark() throws IOException {
    long time = System.currentTimeMillis();
    logWriter = new MLogWriter(config.getSchemaDir(), MetadataConstant.METADATA_LOG + ".temp");
    logWriter.setLogNum(0);
    if (!logFile.exists()) {
      throw new FileNotFoundException("we need a mlog.bin to init the benchmark test");
    }
    try (MLogReader mLogReader =
        new MLogReader(config.getSchemaDir(), MetadataConstant.METADATA_LOG)) {
      parseForTestSet(mLogReader);
      System.out.println("spend " + (System.currentTimeMillis() - time) + "ms to prepare dataset");
    } catch (Exception e) {
      throw new IOException("Failed to parser mlog.bin for err:" + e);
    }
  }

  private static void parseForTestSet(MLogReader mLogReader) throws IllegalPathException {
    List<CreateTimeSeriesPlan> currentList = null;
    SetStorageGroupPlan setStorageGroupPlan = new SetStorageGroupPlan();
    setStorageGroupPlan.setPath(new PartialPath("root.iotcloud"));
    storageGroups.add(setStorageGroupPlan);
    while (mLogReader.hasNext()) {
      PhysicalPlan plan = null;
      try {
        plan = mLogReader.next();
        if (plan == null) {
          continue;
        }
        switch (plan.getOperatorType()) {
          case CREATE_TIMESERIES:
            CreateTimeSeriesPlan createTimeSeriesPlan = (CreateTimeSeriesPlan) plan;
            PartialPath path = createTimeSeriesPlan.getPath();
            if (currentList == null) {
              currentList = new ArrayList<>(BIN_CAPACITY);
              timeSeriesSet.add(currentList);
            }
            measurementPathSet.add(path.getFullPath());

            innerPathSet.add(path.getDevice());
            String[] subNodes = ArrayUtils.subarray(path.getNodes(), 0, path.getNodes().length - 2);
            innerPathSet.add(String.join(".", subNodes));

            currentList.add(createTimeSeriesPlan);
            if (currentList.size() >= BIN_CAPACITY) {
              currentList = null;
            }
            break;
          default:
            break;
        }
      } catch (Exception e) {
        logger.error(
            "Can not operate cmd {} for err:", plan == null ? "" : plan.getOperatorType(), e);
      }
    }
  }

  private void resetEnv() throws IOException {
    File rockdDbFile = new File(ROCKSDB_PATH);
    if (rockdDbFile.exists() && rockdDbFile.isDirectory()) {
      FileUtils.deleteDirectory(rockdDbFile);
    }
  }
}
