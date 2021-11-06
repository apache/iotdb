/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.influxdb.protocol.input;

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
