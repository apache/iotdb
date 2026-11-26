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

package org.apache.iotdb.commons.udf.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class KDTreeUtilTest {

  @Test
  public void testQueryUsesCompleteNodeBounds() {
    ArrayList<ArrayList<Double>> data = new ArrayList<>();
    data.add(point(4, 0));
    data.add(point(-2, -2));
    data.add(point(-5, 5));
    data.add(point(1, -1));
    data.add(point(-4, 5));
    data.add(point(5, -5));
    data.add(point(-5, 0));

    KDTreeUtil tree = KDTreeUtil.build(data, 2);

    Assert.assertEquals(point(5, -5), tree.query(point(0.25, -8.75), new double[] {1, 1}));
  }

  private ArrayList<Double> point(double first, double second) {
    return new ArrayList<>(Arrays.asList(first, second));
  }
}
