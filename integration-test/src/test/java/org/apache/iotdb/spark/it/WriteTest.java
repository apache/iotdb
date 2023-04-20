package org.apache.iotdb.spark.it;

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WriteTest extends AbstractTest {
  private Session session;

  @Before
  @Override
  public void before() throws IoTDBConnectionException, ClassNotFoundException {
    super.before();
    session = new Session(ip, port, "root", "root");
    session.open();
  }

  @After
  @Override
  public void after() throws IoTDBConnectionException {
    session.close();
    super.after();
  }

  @Test
  public void testInsertWideData() throws IoTDBConnectionException, StatementExecutionException {
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1L, 1, 1L, 1.0F, 1.0D, true, "hello"));
    rows.add(RowFactory.create(2L, 2, 2L, 2.0F, 2.0D, false, "world"));

    StructField[] structFields =
        new StructField[] {
          new StructField("Time", DataTypes.LongType, false, Metadata.empty()),
          new StructField("root.test.d0.s0", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("root.test.d0.s1", DataTypes.LongType, true, Metadata.empty()),
          new StructField("root.test.d0.s2", DataTypes.FloatType, true, Metadata.empty()),
          new StructField("root.test.d0.s3", DataTypes.DoubleType, true, Metadata.empty()),
          new StructField("root.test.d0.s4", DataTypes.BooleanType, true, Metadata.empty()),
          new StructField("root.test.d0.s5", DataTypes.StringType, true, Metadata.empty())
        };
    StructType structType = new StructType(structFields);

    Dataset<Row> df = spark.createDataFrame(rows, structType);

    df.write().format("org.apache.iotdb.spark.db").option("url", jdbcUrl).save();

    SessionDataSet result = session.executeQueryStatement("select ** from root");
    int size = 0;
    while (result.hasNext()) {
      result.next();
      size++;
    }
    Assert.assertEquals(2, size);
  }

  @Test
  public void testInsertNarrowData() throws IoTDBConnectionException, StatementExecutionException {
    List<Row> rows = new ArrayList<>();
    rows.add(RowFactory.create(1L, "root.test.d0", 1, 1L, 1.0F, 1.0D, true, "hello"));
    rows.add(RowFactory.create(2L, "root.test.d0", 2, 2L, 2.0F, 2.0D, false, "world"));

    StructField[] structFields =
        new StructField[] {
          new StructField("Time", DataTypes.LongType, false, Metadata.empty()),
          new StructField("Device", DataTypes.StringType, false, Metadata.empty()),
          new StructField("s0", DataTypes.IntegerType, true, Metadata.empty()),
          new StructField("s1", DataTypes.LongType, true, Metadata.empty()),
          new StructField("s2", DataTypes.FloatType, true, Metadata.empty()),
          new StructField("s3", DataTypes.DoubleType, true, Metadata.empty()),
          new StructField("s4", DataTypes.BooleanType, true, Metadata.empty()),
          new StructField("s5", DataTypes.StringType, true, Metadata.empty())
        };
    StructType structType = new StructType(structFields);

    Dataset<Row> df = spark.createDataFrame(rows, structType);

    df.write().format("org.apache.iotdb.spark.db").option("url", jdbcUrl).save();

    SessionDataSet result = session.executeQueryStatement("select ** from root");
    int size = 0;
    while (result.hasNext()) {
      result.next();
      size++;
    }
    Assert.assertEquals(2, size);
  }
}
