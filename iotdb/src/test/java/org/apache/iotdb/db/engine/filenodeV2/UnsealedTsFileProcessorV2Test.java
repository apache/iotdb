package org.apache.iotdb.db.engine.filenodeV2;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.FileSchemaUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UnsealedTsFileProcessorV2Test {

  private UnsealedTsFileProcessorV2 processor;
  private String storageGroup = "storage_group1";
  private String filePath = "testUnsealedTsFileProcessor.tsfile";
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private Map<String, String> props = Collections.emptyMap();

  @Before
  public void setUp() throws Exception {
    MetadataManagerHelper.initMetadata();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    EnvironmentUtils.cleanDir(filePath);
  }

  @Test
  public void testWriteAndFlush() throws WriteProcessException, IOException {
    processor = new UnsealedTsFileProcessorV2(storageGroup, new File(filePath),
        FileSchemaUtils.constructFileSchema(deviceId), SysTimeVersionController.INSTANCE, x->{});

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.write(record);
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props);
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

    // flushMetadata asynchronously
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, props);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(1, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getTsDataType());
  }


  @Test
  public void testMultiFlush() throws WriteProcessException, IOException {
    processor = new UnsealedTsFileProcessorV2(storageGroup, new File(filePath),
        FileSchemaUtils.constructFileSchema(deviceId), SysTimeVersionController.INSTANCE, x->{});

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int flushId = 0; flushId < 100; flushId++) {
      for (int i = 1; i <= 100; i++) {
        TSRecord record = new TSRecord(i, deviceId);
        record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
        processor.write(record);
      }
      processor.asyncFlush();
    }
    processor.syncFlush();

    pair = processor.query(deviceId, measurementId, dataType, props);
    left = pair.left;
    right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(100, right.size());
    assertEquals(measurementId, right.get(0).getMeasurementUid());
    assertEquals(dataType, right.get(0).getTsDataType());
  }


  @Test
  public void testWriteAndClose() throws WriteProcessException, IOException {
    processor = new UnsealedTsFileProcessorV2(storageGroup, new File(filePath),
        FileSchemaUtils.constructFileSchema(deviceId), SysTimeVersionController.INSTANCE, x->{
      TsFileResourceV2 resource = processor.getTsFileResource();
      synchronized (resource) {
        for (Entry<String, Long> startTime : resource.getStartTimeMap().entrySet()) {
          String deviceId = startTime.getKey();
          resource.getEndTimeMap().put(deviceId, resource.getStartTimeMap().get(deviceId));
        }
      }
    });

    Pair<ReadOnlyMemChunk, List<ChunkMetaData>> pair = processor
        .query(deviceId, measurementId, dataType, props);
    ReadOnlyMemChunk left = pair.left;
    List<ChunkMetaData> right = pair.right;
    assertTrue(left.isEmpty());
    assertEquals(0, right.size());

    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, deviceId);
      record.addTuple(DataPoint.getDataPoint(dataType, measurementId, String.valueOf(i)));
      processor.write(record);
    }

    // query data in memory
    pair = processor.query(deviceId, measurementId, dataType, props);
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

    // flushMetadata asynchronously
    processor.syncClose();

    assertTrue(processor.getTsFileResource().isClosed());

  }

}