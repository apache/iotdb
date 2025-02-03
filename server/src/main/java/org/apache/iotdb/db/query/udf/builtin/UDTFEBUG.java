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
package org.apache.iotdb.db.query.udf.builtin;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.eBUG.Point;
import org.apache.iotdb.db.query.eBUG.eBUG;
import org.apache.iotdb.db.query.udf.api.UDTF;
import org.apache.iotdb.db.query.udf.api.access.Row;
import org.apache.iotdb.db.query.udf.api.collector.PointCollector;
import org.apache.iotdb.db.query.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameterValidator;
import org.apache.iotdb.db.query.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.db.query.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.db.query.udf.api.exception.UDFException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UDTFEBUG implements UDTF {

  protected TSDataType dataType;

  private int m; // target number of sampled points

  private int e;

  private List<Point> points;

  @Override
  public void validate(UDFParameterValidator validator) throws UDFException {
    validator
        .validateInputSeriesNumber(1)
        .validateInputSeriesDataType(
            0, TSDataType.INT32, TSDataType.INT64, TSDataType.FLOAT, TSDataType.DOUBLE)
        .validateRequiredAttribute("m");
  }

  @Override
  public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations)
      throws MetadataException {
    dataType = parameters.getDataType(0);
    m = parameters.getInt("m"); // m>2 for online sampling mode, <=2 for precomputation mode
    //        if (m <= 2) {
    //            throw new MetadataException("m should be larger than 2 for online sampling mode");
    //        }
    e = parameters.getInt("e");
    if (e < 0) {
      throw new MetadataException("e should not be smaller than 0");
    }

    if (m <= 2) { // precomputation mode
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.TEXT);
    } else { // online sampling mode
      configurations
          .setAccessStrategy(new RowByRowAccessStrategy())
          .setOutputDataType(TSDataType.DOUBLE);
    }
    points = new ArrayList<>();
  }

  @Override
  public void transform(Row row, PointCollector collector)
      throws QueryProcessException, IOException {
    double t = row.getTime();
    switch (dataType) {
      case INT32:
        points.add(new Point(t, row.getInt(0)));
        break;
      case INT64:
        points.add(new Point(t, row.getLong(0)));
        break;
      case FLOAT:
        points.add(new Point(t, row.getFloat(0)));
        break;
      case DOUBLE:
        points.add(new Point(t, row.getDouble(0)));
        break;
      default:
        break;
    }
  }

  @Override
  public void terminate(PointCollector collector) throws IOException, QueryProcessException {
    List<Point> sampled = eBUG.buildEffectiveArea(points, e, false, m);

    // collect result
    if (m <= 2) {
      // offline precomputation mode, for precomputing the dominated significance of each, ordered
      // by dominated sig in descending order (i.e., reverse of bottom-up elimination order)
      for (int i = 0; i < sampled.size(); i++) {
        collector.putString(i + 1, sampled.get(i).toString());
        // not using dominated significance z as timestamp,
        // because dominated significance z can be double, if converted to long may lose some order
        // information
      }
    } else {
      // m>2, online sampling mode, order by time in ascending order
      for (Point p : sampled) {
        collector.putDouble((long) p.getTimestamp(), p.getValue());
      }
    }
  }
}
