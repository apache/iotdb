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
package org.apache.iotdb.library.drepair.util;

import org.apache.iotdb.db.query.udf.api.access.RowIterator;

/**
 * 用于填补时间序列缺失值的UDTF：利用前一个数据点的值进行填补。
 *
 * @author Wang Haoyu & Zhang Xiaojian
 */
public class PreviousFill extends ValueFill {

  private long previousTime = -1;
  private double previousValue;

  public PreviousFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
  }

  public PreviousFill(String filename) throws Exception {
    super(filename);
  }

  @Override
  public void fill() {
    // 如果开头是NaN，则直接忽略不考虑；-> 这个规则参照pd.Dataframe.fillna函数的定义
    for (int i = 0; i < original.length; i++) {
      if (Double.isNaN(original[i])) {
        if (previousTime == -1) { // 序列初始为空，直接pass
          repaired[i] = original[i];
        } else {
          repaired[i] = previousValue;
        }
      } else {
        previousTime = time[i];
        previousValue = original[i];
        repaired[i] = original[i];
      }
    }
  }
  //    public void beforeStart(UDFParameters udfp, UDTFConfigurations udtfc) throws Exception {
  //        udtfc.setAccessStrategy(new RowByRowAccessStrategy())
  //                .setOutputDataType(udfp.getDataType(0));
  //        beforeRange = udfp.getLongOrDefault("beforeRange", Long.MAX_VALUE);
  //    }
  //
  //    public void transform(Row row, PointCollector pc) throws Exception {
  //        switch (row.getDataType(0)) {
  //            case DOUBLE:
  //                if (row.isNull(0) || Double.isNaN(row.getDouble(0))) {//需要进行填补
  //                    if (previousTime > 0 && row.getTime() - previousTime <= beforeRange)
  // {//可以进行填补
  //                        pc.putDouble(row.getTime(), previousValue1);
  //                    } else {
  //                        pc.putDouble(row.getTime(), Double.NaN);
  //                    }
  //                } else {//不需要进行填补
  //                    previousTime = row.getTime();
  //                    previousValue1 = row.getDouble(0);
  //                    pc.putDouble(previousTime, previousValue1);
  //                }
  //                break;
  //            case FLOAT:
  //                if (row.isNull(0) || Float.isNaN(row.getFloat(0))) {//需要进行填补
  //                    if (previousTime > 0 && row.getTime() - previousTime <= beforeRange)
  // {//可以进行填补
  //                        pc.putFloat(row.getTime(), previousValue2);
  //                    } else {
  //                        pc.putFloat(row.getTime(), Float.NaN);
  //                    }
  //                } else {//不需要进行填补
  //                    previousTime = row.getTime();
  //                    previousValue2 = row.getFloat(0);
  //                    pc.putFloat(previousTime, previousValue2);
  //                }
  //                break;
  //            default:
  //                throw new Exception();
  //        }
  //    }
}
