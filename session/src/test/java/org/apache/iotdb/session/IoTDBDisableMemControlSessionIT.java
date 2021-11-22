package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class IoTDBDisableMemControlSessionIT {

  private static Logger logger = LoggerFactory.getLogger(IoTDBDisableMemControlSessionIT.class);

  private Session session;

  @Before
  public void setUp() {
    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    EnvironmentUtils.closeStatMonitor();
    IoTDBDescriptor.getInstance().getConfig().setEnableMemControl(false);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    session.close();
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setEnableMemControl(true);
  }

  @Test
  public void testInsertPartialTablet()
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open();

    if (!session.checkTimeseriesExists("root.sg.d.s1")) {
      session.createTimeseries(
          "root.sg.d.s1", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists("root.sg.d.s2")) {
      session.createTimeseries(
          "root.sg.d.s2", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists("root.sg.d.s3")) {
      session.createTimeseries(
          "root.sg.d.s3", TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.DOUBLE));
    schemaList.add(new MeasurementSchema("s3", TSDataType.TEXT));

    Tablet tablet = new Tablet("root.sg.d", schemaList, 10);

    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 15; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s1", rowIndex, 1L);
      tablet.addValue("s2", rowIndex, 1D);
      tablet.addValue("s3", rowIndex, new Binary("1"));
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        try {
          session.insertTablet(tablet, true);
        } catch (StatementExecutionException e) {
          Assert.assertEquals(
              "313: failed to insert measurements [s3] caused by DataType mismatch, Insert measurement s3 type TEXT, metadata tree type INT64",
              e.getMessage());
        }
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      try {
        session.insertTablet(tablet);
      } catch (StatementExecutionException e) {
        Assert.assertEquals(
            "313: failed to insert measurements [s3] caused by DataType mismatch, Insert measurement s3 type TEXT, metadata tree type INT64",
            e.getMessage());
      }
      tablet.reset();
    }

    SessionDataSet dataSet =
        session.executeQueryStatement("select count(s1), count(s2), count(s3) from root.sg.d");
    while (dataSet.hasNext()) {
      RowRecord rowRecord = dataSet.next();
      Assert.assertEquals(15L, rowRecord.getFields().get(0).getLongV());
      Assert.assertEquals(15L, rowRecord.getFields().get(1).getLongV());
      Assert.assertEquals(0L, rowRecord.getFields().get(2).getLongV());
    }
    session.close();
  }
}
