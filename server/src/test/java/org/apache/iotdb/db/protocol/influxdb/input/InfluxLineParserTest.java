package org.apache.iotdb.db.protocol.influxdb.input;

import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class InfluxLineParserTest {
  @Test
  public void parseToPointTest() {
    String[] records = {
      "student,name=xie,sex=m country=\"china\",score=87.0,tel=\"110\" 1635177018815000000",
      "student,name=xie,sex=m country=\"china\",score=87i,tel=990i 1635187018815000000",
      "cpu,name=xie country=\"china\",score=100.0 1635187018815000000"
    };
    int expectLength = 3;
    for (int i = 0; i < expectLength; i++) {
      Assert.assertEquals(records[i], InfluxLineParser.parseToPoint(records[i]).lineProtocol());
    }
  }

  @Test
  public void parserRecordsToPoints() {
    String[] records = {
      "student,name=xie,sex=m country=\"china\",score=87.0,tel=\"110\" 1635177018815000000",
      "student,name=xie,sex=m country=\"china\",score=87i,tel=990i 1635187018815000000",
      "cpu,name=xie country=\"china\",score=100.0 1635187018815000000"
    };
    int expectLength = 3;
    ArrayList<Point> points =
        (ArrayList<Point>) InfluxLineParser.parserRecordsToPoints(String.join("\n", records));
    for (int i = 0; i < expectLength; i++) {
      Assert.assertEquals(records[i], points.get(i).lineProtocol());
    }
  }
}
