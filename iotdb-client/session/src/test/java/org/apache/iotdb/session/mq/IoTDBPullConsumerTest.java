package org.apache.iotdb.session.mq;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Array;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IoTDBPullConsumerTest {
  private IoTDBPollConsumer pollConsumer;

  @Before
  public void before()
      throws IoTDBConnectionException, URISyntaxException, InterruptedException,
          StatementExecutionException {
    pollConsumer = new IoTDBPollConsumer.Builder().build();
    pollConsumer.open();
  }

  @Test
  public void testConsume()
      throws IoTDBConnectionException, InterruptedException, StatementExecutionException {
    pollConsumer.subscribe("topic_root");
    for (int i = 0; i < 10; i++) {
      TabletWrapper wrapper = pollConsumer.poll(10);
      if (wrapper != null) {
        printTablet(wrapper.getTablet());
      }
    }
  }

  @After
  public void after() throws IoTDBConnectionException {
    pollConsumer.unsubscribe();
    pollConsumer.close();
  }

  private void printTablet(Tablet tablet) {
    List<MeasurementSchema> schemas = tablet.getSchemas();
    int rowSize = tablet.rowSize;
    HashMap<String, Pair<BitMap, List<Object>>> values = new HashMap<>();
    List<String> timeseriesList = new ArrayList<>();
    timeseriesList.add("Time");
    for (MeasurementSchema schema : schemas) {
      String timeseries = String.format("%s.%s", tablet.deviceId, schema.getMeasurementId());
      timeseriesList.add(timeseries);
      values.put(
          timeseries,
          new Pair<>(
              tablet.bitMaps[schemas.indexOf(schema)],
              object2List(tablet.values[schemas.indexOf(schema)])));
    }
    System.out.println(StringUtils.joinWith("\t", timeseriesList));
    for (int i = 0; i < rowSize; i++) {
      ArrayList<Object> row = new ArrayList<>();
      row.add(tablet.timestamps[i]);
      for (String timeseries : timeseriesList) {
        if ("Time".equals(timeseries)) {
          continue;
        }
        if (values.containsKey(timeseries)
            && (values.get(timeseries).getLeft() == null
                || !values.get(timeseries).getLeft().isMarked(i))) {
          row.add(values.get(timeseries).getRight().get(i));
        } else {
          row.add(null);
        }
      }
      System.out.println(StringUtils.joinWith("\t", row));
    }
  }

  public static List<Object> object2List(Object obj) {
    ArrayList<Object> objects = new ArrayList<>();
    int length = Array.getLength(obj);
    for (int i = 0; i < length; i++) {
      objects.add(Array.get(obj, i));
    }
    return objects;
  }
}
