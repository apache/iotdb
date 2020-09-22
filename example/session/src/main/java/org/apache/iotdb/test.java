package org.apache.iotdb.tsfile.write;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class test {
  Map<String, Map<String, Integer>> table = new HashMap<>();
  private static Session session;
  public static void main(String[] args) throws IOException {
    Map<String, Integer> map = new HashMap<>();
    System.out.println(map.get("444"));
   /* MeasurementSchema measurementSchema = new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    measurementSchema.serializeTo(outputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(outputStream.toByteArray());
    MeasurementSchema mm = MeasurementSchema.deserializeFrom(byteBuffer);
    System.out.println(mm);*/
  }
  void labelOrderMap(Set<String> labelKeys, String metricName)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);
    Map<String, Integer> keyOrderMapInMetric = table.get(metricName);
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    if (keyOrderMapInMetric == null ) {
      //it is a new metric
      measurements.add("metric_name");
      types.add(TSDataType.TEXT);
      //add <timestamp, metric> into root.TAG_INFO.metric_name
      session.insertRecord("LABEL_INFO", System.currentTimeMillis(), measurements, types);
      //then,
      /*keyOrderMapInMetric = empty*/
    }
    for (String label : labelKeys) {
      Integer labelOrder = keyOrderMapInMetric.get(label);
      if (labelOrder == null) {
        // it is a new tag
       /* Integer largest = */
       /* largest = find the largest order in this metric,
            add <timestamp, tag> into `root.TAG_INFO.tag_name`
        add <timestamp, largest + 1 >  into `root.TAG_INFO.tag_order`*/
      }
    }
  }
}
