package org.apache.iotdb.db.metadata.id_table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.memtable.IWritableMemChunkGroup;
import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.id_table.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.id_table.entry.IDeviceID;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IDTableResourceControlTest {
  private final Planner processor = new Planner();

  private boolean isEnableIDTable = false;

  @Before
  public void setUp() {
    isEnableIDTable = IoTDBDescriptor.getInstance().getConfig().isEnableIDTable();
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setEnableIDTable(isEnableIDTable);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDeviceIDReusing()
      throws QueryProcessException, MetadataException, InterruptedException,
          QueryFilterOptimizationException, StorageEngineException, IOException {
    InsertRowPlan rowPlan = getInsertRowPlan();

    PlanExecutor executor = new PlanExecutor();
    executor.insert(rowPlan);

    QueryPlan queryPlan = (QueryPlan) processor.parseSQLToPhysicalPlan("select * from root.isp.d1");
    QueryDataSet dataSet = executor.processQuery(queryPlan, EnvironmentUtils.TEST_QUERY_CONTEXT);
    Assert.assertEquals(6, dataSet.getPaths().size());
    while (dataSet.hasNext()) {
      RowRecord record = dataSet.next();
      Assert.assertEquals(6, record.getFields().size());
    }

    IDeviceID idTableDeviceID = null;
    for (Map<IDeviceID, DeviceEntry> map :
        StorageEngine.getInstance()
            .getProcessor(new PartialPath("root.isp.d1"))
            .getIdTable()
            .getIdTables()) {
      if (map == null) {
        continue;
      }

      for (IDeviceID deviceID : map.keySet()) {
        if (idTableDeviceID == null) {
          idTableDeviceID = deviceID;
        } else {
          fail("there should only be one device in id table");
        }
      }
    }

    assertNotNull(idTableDeviceID);

    int deviceCount = 0;
    for (TsFileProcessor processor :
        StorageEngine.getInstance()
            .getProcessor(new PartialPath("root.isp.d1"))
            .getWorkSequenceTsFileProcessors()) {
      for (Map.Entry<IDeviceID, IWritableMemChunkGroup> entry :
          processor.getWorkMemTable().getMemTableMap().entrySet()) {
        // using '!=' to check is same device id
        if (entry.getKey() != rowPlan.getDeviceID()) {
          fail("memtable's device id is not same as insert plan's device id");
        }

        // using '!=' to check is same device id
        if (entry.getKey() != idTableDeviceID) {
          fail("memtable's device id is not same as insert plan's device id");
        }

        deviceCount++;
      }
    }

    assertEquals(1, deviceCount);
  }

  private InsertRowPlan getInsertRowPlan() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    String[] columns = new String[6];
    columns[0] = 1.0 + "";
    columns[1] = 2 + "";
    columns[2] = 10000 + "";
    columns[3] = 100 + "";
    columns[4] = false + "";
    columns[5] = "hh" + 0;

    return new InsertRowPlan(
        new PartialPath("root.isp.d1"),
        time,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        columns);
  }
}
