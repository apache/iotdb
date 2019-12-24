package org.apache.iotdb.db.engine.storagegroup;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.nvm.space.NVMSpaceManager;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.Test;

public class PerfTest {

  private static TsFileProcessor processor;
  private static String storageGroup = "storage_group1";
  private static String filePath = "data/testUnsealedTsFileProcessor.tsfile";
  private static String deviceId = "root.vehicle.d0";
  private static String measurementId = "s0";
  private static TSDataType dataType = TSDataType.INT32;
  private static Map<String, String> props = Collections.emptyMap();
  private static QueryContext context;

  private static TsFileProcessor processor2;
  private static String storageGroup2 = "storage_group2";
  private static String filePath2 = "data/testUnsealedTsFileProcessor.tsfile2";
  private static String deviceId2 = "root.vehicle.d2";
  private static String measurementId2 = "s2";
  private static TSDataType dataType2 = TSDataType.INT32;
  private static Map<String, String> props2 = Collections.emptyMap();
  private static QueryContext context2;

  public static void main(String[] args)
      throws IOException, StartupException, StorageEngineException, QueryProcessException {
    args = new String[]{"data/testUnsealedTsFileProcessor.tsfile", "data/testUnsealedTsFileProcessor.tsfile2", "data/nvmFile", "true", "10000"};
    filePath = args[0];
    filePath2 = args[1];
    String nvmPath = args[2];
    IoTDBDescriptor.getInstance().getConfig().setEnableWal(Boolean.parseBoolean(args[3]));
    IoTDBDescriptor.getInstance().getConfig().setFlushWalThreshold(Integer.parseInt(args[4]));

    int recordNum = 100000;
    long time;

    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
    context = EnvironmentUtils.TEST_QUERY_CONTEXT;
    context2 = EnvironmentUtils.TEST_QUERY_CONTEXT;

    processor = new TsFileProcessor(storageGroup, SystemFileFactory.INSTANCE.getFile(filePath),
        SchemaUtils.constructSchema(deviceId), SysTimeVersionController.INSTANCE, x -> {
    },
        () -> true, true);

    processor2 = new TsFileProcessor(storageGroup2, SystemFileFactory.INSTANCE.getFile(filePath2),
        SchemaUtils.constructSchema(deviceId2), SysTimeVersionController.INSTANCE, x -> {
    },
        () -> true, true);
    processor2.setUseNVM(true);

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props, context);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair2 = processor2
        .query(deviceId2, measurementId2, dataType2, props2, context2);
    ReadOnlyMemChunk left2 = pair.left;
    List<ChunkMetaData> right2 = pair.right;
    assertTrue(left2.isEmpty());
    assertEquals(0, right2.size());

    System.out.println("insert start");

    time = System.currentTimeMillis();
    for (int i = 1; i <= recordNum; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.insert(new InsertPlan(record));
    }
    System.out.println("normal:" + (System.currentTimeMillis() - time));

    time = System.currentTimeMillis();
    for (int i = 1; i <= recordNum; i++) {
      TSRecord record = new TSRecord(i, deviceId2);
      record.addTuple(DataPoint.getDataPoint(dataType2, measurementId2, String.valueOf(i)));
      processor2.insert(new InsertPlan(record));
    }
    System.out.println("nvm:" + (System.currentTimeMillis() - time));

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props, context);
    left = pair.left;
    assertFalse(left.isEmpty());
    int num = 1;
    Iterator<TimeValuePair> iterator = left.getIterator();
    for (; num <= 100; num++) {
      iterator.hasNext();
      TimeValuePair timeValuePair = iterator.next();
      assertEquals(num, timeValuePair.getTimestamp());
      assertEquals(num, timeValuePair.getValue().getInt());
    }

    pair2 = processor2.query(deviceId2, measurementId2, dataType2, props2, context2);
    left2 = pair2.left;
    assertFalse(left2.isEmpty());
    int num2 = 1;
    Iterator<TimeValuePair> iterator2 = left2.getIterator();
    for (; num2 <= 100; num2++) {
      iterator2.hasNext();
      TimeValuePair timeValuePair2 = iterator2.next();
      assertEquals(num2, timeValuePair2.getTimestamp());
      assertEquals(num2, timeValuePair2.getValue().getInt());
    }


    EnvironmentUtils.cleanEnv();
    new File(filePath).deleteOnExit();
    new File(filePath2).deleteOnExit();
    new File(nvmPath).deleteOnExit();
    System.exit(0);
    System.out.println("exit");
  }
}
