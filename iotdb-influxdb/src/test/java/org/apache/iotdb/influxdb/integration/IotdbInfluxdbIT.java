package org.apache.iotdb.influxdb.integration;

import org.apache.iotdb.influxdb.IotDBInfluxDB;

import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class IotdbInfluxdbIT {

  @Rule
  public GenericContainer IotDB =
      new GenericContainer("apache/iotdb:latest").withExposedPorts(6667);

  private IotDBInfluxDB iotDBInfluxDB;

  @Before
  public void setUp() {
    iotDBInfluxDB =
        new IotDBInfluxDB(IotDB.getContainerIpAddress(), IotDB.getMappedPort(6667), "root", "root");
    iotDBInfluxDB.createDatabase("database");
    iotDBInfluxDB.setDatabase("database");
    insertData();
  }

  private void insertData() {
    // insert the build parameter to construct the influxdb
    Point.Builder builder = Point.measurement("student");
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    fields.put("score", 87);
    fields.put("tel", "110");
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    Point point = builder.build();
    // after the build construction is completed, start writing
    iotDBInfluxDB.write(point);

    builder = Point.measurement("student");
    tags = new HashMap<>();
    fields = new HashMap<>();
    tags.put("name", "xie");
    tags.put("sex", "m");
    tags.put("province", "anhui");
    fields.put("score", 99);
    fields.put("country", "china");
    builder.tag(tags);
    builder.fields(fields);
    builder.time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    point = builder.build();
    iotDBInfluxDB.write(point);
  }

  @Test
  public void testCommonQueryColumn() {
    Query query =
        new Query(
            "select * from student where (name=\"xie\" and sex=\"m\")or time<now()-7d", "database");
    QueryResult result = iotDBInfluxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);

    String[] retArray = new String[] {"time", "name", "sex", "province", "country", "score", "tel"};
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertEquals(retArray[i], series.getColumns().get(i));
    }
  }

  @Test
  public void testFuncWithoutFilter() {
    Query query =
        new Query(
            "select max(score),min(score),sum(score),count(score),spread(score),mean(score),first(score),last(score) from student ",
            "database");
    QueryResult result = iotDBInfluxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);

    Object[] retArray = new Object[] {0, 99.0, 87.0, 186, 2, 12.0, 93, 87, 99};
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertEquals(retArray[i], series.getValues().get(0).get(i));
    }
  }

  @Test
  public void testFunc() {
    Query query =
        new Query(
            "select count(score),first(score),last(country),max(score),mean(score),median(score),min(score),mode(score),spread(score),stddev(score),sum(score) from student where (name=\"xie\" and sex=\"m\")or score<99",
            "database");
    QueryResult result = iotDBInfluxDB.query(query);
    QueryResult.Series series = result.getResults().get(0).getSeries().get(0);
    System.out.println("qqq" + series.toString());

    Object[] retArray =
        new Object[] {0, 2, 87, "china", 99.0, 93.0, 93.0, 87.0, 87, 12.0, 6.0, 186.0};
    for (int i = 0; i < series.getColumns().size(); i++) {
      assertEquals(retArray[i], series.getValues().get(0).get(i));
    }
  }
}
