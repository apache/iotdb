package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.write.SchemaRegionWritePlanFactory;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngine;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Tests {

  protected ISchemaRegion getSchemaRegion(String database, int schemaRegionId) throws Exception {
    SchemaRegionId regionId = new SchemaRegionId(schemaRegionId);
    if (SchemaEngine.getInstance().getSchemaRegion(regionId) == null) {
      SchemaEngine.getInstance().createSchemaRegion(new PartialPath(database), regionId);
    }
    return SchemaEngine.getInstance().getSchemaRegion(regionId);
  }

  @Before
  public void setup() {
    SchemaEngine.getInstance().init();
  }

  @After
  public void teardown() throws IOException {
    SchemaEngine.getInstance().clear();
    FileUtils.deleteDirectory(new File(IoTDBDescriptor.getInstance().getConfig().getSchemaDir()));
  }

  @Test
  public void testFetchSchema() throws Exception {
    ISchemaRegion schemaRegion = getSchemaRegion("root.ln.wf01.wt01", 0);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.ln.wf01.wt01.status"),
            TSDataType.BOOLEAN,
            TSEncoding.PLAIN,
            CompressionType.SNAPPY,
            null,
            null,
            null,
            null),
        -1);
    schemaRegion.createTimeseries(
        SchemaRegionWritePlanFactory.getCreateTimeSeriesPlan(
            new PartialPath("root.ln.wf01.wt01.temperature"),
            TSDataType.FLOAT,
            TSEncoding.RLE,
            CompressionType.SNAPPY,
            new HashMap<String, String>() {
              {
                put("MAX_POINT_NUMBER", "3");
              }
            },
            null,
            null,
            null),
        -1);

    ISchemaRegion schemaRegion2 = getSchemaRegion("root.ln.wf01.wt02", 1);
    Thread.sleep(1000);
    schemaRegion2.createAlignedTimeSeries(
        SchemaRegionWritePlanFactory.getCreateAlignedTimeSeriesPlan(
            new PartialPath("root.ln.wf01.wt02"),
            Arrays.asList("s1", "s2"),
            Arrays.asList(TSDataType.INT32, TSDataType.DOUBLE),
            Arrays.asList(TSEncoding.RLE, TSEncoding.RLE),
            Arrays.asList(CompressionType.SNAPPY, CompressionType.SNAPPY),
            null,
            null,
            null));
  }

  public static void main(String[] args) {
    ExecutorService flushTaskExecutor1 =
        //            IoTDBThreadPoolFactory.newFixedThreadPool(5, "1");
        Executors.newFixedThreadPool(5);
    List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
    for (int i = 0; i < 3; i++) {
      CompletableFuture<Void>[] completableFutures = new CompletableFuture[5];
      for (int j = 0; j < 5; j++) {
        completableFutures[j] =
            CompletableFuture.runAsync(
                () -> {
                  System.out.println("run");
                },
                flushTaskExecutor1);
      }
      CompletableFuture.allOf(completableFutures)
          .whenComplete(
              (res, throwable) -> {
                for (CompletableFuture f : completableFutures) {
                  f.complete("");
                }
                System.out.println("done");
              })
          .join();
    }
  }
}
