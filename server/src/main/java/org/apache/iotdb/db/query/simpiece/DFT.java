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

package org.apache.iotdb.db.query.simpiece;

import org.apache.iotdb.db.query.simpiece.jwave.Transform;
import org.apache.iotdb.db.query.simpiece.jwave.TransformBuilder;

import java.util.ArrayList;
import java.util.List;

public class DFT {

  public static List<Point> reducePoints(double[] values, double threshold) {
    Transform transform = TransformBuilder.create("Discrete Fourier Transform", "Haar");
    double[] arrHilb = transform.forward(values);

    List<Point> result = new ArrayList<>();
    int n = arrHilb.length;
    for (int i = 0; i < n; i++) {
      if (Math.abs(arrHilb[i]) >= threshold) {
        result.add(new Point(i, arrHilb[i]));
      }
    }
    return result;
  }
}
