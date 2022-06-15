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

package org.apache.iotdb.library.dquality;

import org.apache.iotdb.library.dquality.util.TimeSeriesQuality;
import org.apache.iotdb.library.util.NoNumberException;
import org.apache.iotdb.library.util.Util;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.RowWindow;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This function calculates timeliness of input series. */
public class UDTFTimeliness implements UDTF {
  @Override
  public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
    boolean isTime = false;
    long window = Integer.MAX_VALUE;
    if (udfp.hasAttribute("window")) {
      String s = udfp.getString("window");
      window = Util.parseTime(s);
      if (window > 0) {
        isTime = true;
      } else {
        window = Long.parseLong(s);
      }
    }
    if (isTime) {
      udtfc.setAccessStrategy(new SlidingTimeWindowAccessStrategy(window));
    } else {
      udtfc.setAccessStrategy(new SlidingSizeWindowAccessStrategy((int) window));
    }
    udtfc.setOutputDataType(Type.DOUBLE);
  }

  @Override
  public void transform(RowWindow rowWindow, PointCollector collector) throws Exception {
    try {
      if (rowWindow.windowSize() > TimeSeriesQuality.windowSize) {
        TimeSeriesQuality tsq = new TimeSeriesQuality(rowWindow.getRowIterator());
        tsq.timeDetect();
        collector.putDouble(rowWindow.getRow(0).getTime(), tsq.getTimeliness());
      }
    } catch (IOException | NoNumberException ex) {
      Logger.getLogger(UDTFCompleteness.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}
