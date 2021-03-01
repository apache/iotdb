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

package org.apache.iotdb.db.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class MathQueryDataSetUtilsTest {

  @Test
  public void roundFloatWithGivenPrecision() {
    int[] size = new int[] {0, 4, 19};
    float[] data =
        new float[] {
          0.0f, (float) Math.PI, (float) (10 * Math.PI), Integer.MAX_VALUE, -Integer.MAX_VALUE
        };

    float[][] expData =
        new float[][] {
          {0.0f, 3.0f, 31f, Integer.MAX_VALUE, -Integer.MAX_VALUE},
          {0.0f, 3.1416f, 31.4159f, Integer.MAX_VALUE, -Integer.MAX_VALUE},
          {0.0f, (float) Math.PI, (float) (10 * Math.PI), Integer.MAX_VALUE, -Integer.MAX_VALUE},
        };
    for (int j = 0; j < size.length; j++) {
      for (int i = 0; i < data.length; i++) {
        float res = MathUtils.roundWithGivenPrecision(data[i], size[j]);
        assertEquals(expData[j][i], res, Float.MIN_NORMAL);
      }
    }
  }

  @Test
  public void roundDoubleWithGivenPrecision() {
    int[] size = new int[] {0, 4, 19};
    double[] data = new double[] {0.0, Math.PI, 10 * Math.PI, Long.MAX_VALUE, -Long.MAX_VALUE};

    double[][] expData =
        new double[][] {
          {0.0, 3.0, 31, Long.MAX_VALUE, -Long.MAX_VALUE},
          {0.0, 3.1416, 31.4159, Long.MAX_VALUE, -Long.MAX_VALUE},
          {0.0, Math.PI, 10 * Math.PI, Long.MAX_VALUE, -Long.MAX_VALUE},
        };
    for (int j = 0; j < size.length; j++) {
      for (int i = 0; i < data.length; i++) {
        double res = MathUtils.roundWithGivenPrecision(data[i], size[j]);
        assertEquals(expData[j][i], res, Double.MIN_NORMAL);
      }
    }
  }
}
