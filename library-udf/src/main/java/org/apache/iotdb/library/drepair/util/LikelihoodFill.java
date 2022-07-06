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

import org.apache.iotdb.udf.api.access.RowIterator;

import java.util.ArrayList;
import java.util.List;

public class LikelihoodFill extends ValueFill {

  private double stopErrorRatio = 0.0001;
  private int stopIteration = Integer.MAX_VALUE;

  public LikelihoodFill(RowIterator dataIterator) throws Exception {
    super(dataIterator);
  }

  @Override
  public void fill() {
    // find missing points
    List<Integer> repairIndexList = new ArrayList<>();
    List<Double> repairValueList = new ArrayList<>();
    // initialize linear interpolation
    int previousNotNaN = -1;
    for (int i = 0; i < n; i++) {
      if (Double.isNaN(original[i])) {
        repairIndexList.add(i);
        repaired[i] = original[i];
      } else {
        if (previousNotNaN >= 0 && previousNotNaN + 1 != i) {
          double delta =
              (original[i] - original[previousNotNaN]) / (time[i] - time[previousNotNaN]);
          for (int j = previousNotNaN + 1; j < i; j++) {
            repaired[j] = original[previousNotNaN] + delta * (time[j] - time[previousNotNaN]);
            repairValueList.add(repaired[j]);
          }
        }
        repaired[i] = original[i];
        previousNotNaN = i;
      }
    }
    // update filled values iteratively
    double errorRatio = 1;
    int iteration = 0;
    while (errorRatio > stopErrorRatio && iteration < stopIteration) {
      errorRatio = 0.0;
      // find best repair from last iteration
      for (int i = 0; i < repairIndexList.size(); i++) {
        int currentIndex = repairIndexList.get(i);
        // no repair for data at beginning and end
        if (currentIndex == 0
            || Double.isNaN(repaired[currentIndex - 1])
            || currentIndex == n - 1
            || Double.isNaN(repaired[currentIndex + 1])) {
          continue;
        }
        double intervalPrev1 = (double) (time[currentIndex] - time[currentIndex - 1]);
        double intervalPost1 = (double) (time[currentIndex + 1] - time[currentIndex]);
        double squareAPrev = 0.0, squareBPrev = 0.0;
        if (currentIndex >= 2 && !Double.isNaN(repaired[currentIndex - 2])) {
          double intervalPrev2 = (double) (time[currentIndex - 2] - time[currentIndex - 1]);
          squareAPrev = 1.0 / (intervalPrev1 * intervalPrev1);
          squareBPrev =
              2.0 * repaired[currentIndex - 2] / (intervalPrev2 * intervalPrev1)
                  - 2.0
                      * (intervalPrev2 + intervalPrev1)
                      * repaired[currentIndex - 1]
                      / (intervalPrev2 * intervalPrev1 * intervalPrev1);
        }
        double squareACurr =
            (intervalPrev1 + intervalPost1)
                * (intervalPrev1 + intervalPost1)
                / (intervalPrev1 * intervalPrev1 * intervalPost1 * intervalPost1);
        double squareBCurr =
            -2.0
                    * (intervalPrev1 + intervalPost1)
                    * repaired[currentIndex - 1]
                    / (intervalPrev1 * intervalPrev1 * intervalPost1)
                - 2.0
                    * (intervalPrev1 + intervalPost1)
                    * repaired[currentIndex + 1]
                    / (intervalPrev1 * intervalPost1 * intervalPost1);
        double squareAPost = 0.0, squareBPost = 0.0;
        if (currentIndex <= n - 3 && !Double.isNaN(repaired[currentIndex + 2])) {
          double intervalPost2 = (double) (time[currentIndex + 2] - time[currentIndex + 1]);
          squareAPost = 1.0 / (intervalPost1 * intervalPost1);
          squareBPost =
              2.0 * repaired[currentIndex + 2] / (intervalPost1 * intervalPost2)
                  - 2.0
                      * (intervalPost1 + intervalPost2)
                      * repaired[currentIndex + 1]
                      / (intervalPost1 * intervalPost1 * intervalPost2);
        }
        // minimize likelihood
        repairValueList.set(
            i,
            -(squareBPrev + squareBCurr + squareBPost)
                / (2.0 * (squareAPrev + squareACurr + squareAPost)));
      }
      for (int i = 0; i < repairIndexList.size(); i++) {
        int currentIndex = repairIndexList.get(i);
        double previousRepair = repaired[currentIndex];
        double updatedRepair = repairValueList.get(i);
        double updatedRatio = Math.abs(updatedRepair - previousRepair) / previousRepair;
        errorRatio = Math.max(updatedRatio, errorRatio);
        repaired[currentIndex] = updatedRepair;
      }
      iteration++;
    }
  }
}
