package org.apache.iotdb.db.metadata.metafile;

import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;

import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MeasurementFileTest {

  private static String BASE_PATH = MeasurementFileTest.class.getResource("").getPath();
  private static String MEASUREMENT_FILEPATH = BASE_PATH + "measurement.txt";

  private MeasurementFile measurementFile;

  @Before
  public void setUp() throws IOException {
    File file = new File(MEASUREMENT_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
    measurementFile = new MeasurementFile(MEASUREMENT_FILEPATH);
  }

  @After
  public void tearDown() throws IOException {
    measurementFile.close();
    File file = new File(MEASUREMENT_FILEPATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testRW() throws IOException {
    MeasurementMNode m1 = new MeasurementMNode(null, "ts1", new MeasurementSchema(), null);
    MeasurementMNode m2 = new MeasurementMNode(null, "ts2", new MeasurementSchema(), null);
    Map<String, String> props = new HashMap<>();
    props.put("a", "1");
    m1.getSchema().setProps(props);
    measurementFile.write(m1);
    MeasurementMNode temp = measurementFile.read(m1.getPosition());
    System.out.println(m1.getName());
    System.out.println(m1.getSchema().getProps().get("a"));
    measurementFile.write(m2);
    temp = measurementFile.read(m2.getPosition());
    System.out.println(m2.getName());
    System.out.println(m2.getSchema().getProps());
  }
}
