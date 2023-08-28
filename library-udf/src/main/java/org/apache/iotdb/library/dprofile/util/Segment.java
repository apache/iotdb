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

package org.apache.iotdb.library.dprofile.util;

import org.apache.iotdb.library.util.LinearRegression;

import org.apache.commons.lang3.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Util for segment. */
public class Segment {
  private Segment() {
    throw new IllegalStateException("Utility class");
  }

  private static double calculateError(double[] y1, double[] y2) throws Exception {
    double[] y = ArrayUtils.addAll(y1, y2);
    int l = y.length;
    double[] x = new double[l];
    for (int i = 0; i < l; i++) {
      x[i] = i;
    }
    LinearRegression linearFit = new LinearRegression(x, y);
    return linearFit.getMAbsE();
  }

  public static List<double[]> bottomUp(double[] value, double maxError) throws Exception {
    List<double[]> segTs = new ArrayList<>();
    if (value.length <= 3) {
      List<double[]> ret = new ArrayList<>();
      ret.add(value);
      return ret;
    }
    if (value.length % 2 == 0) {
      for (int i = 0; i < value.length; i += 2) {
        segTs.add(Arrays.copyOfRange(value, i, i + 2));
      }
    } else {
      for (int i = 0; i < value.length - 3; i += 2) {
        segTs.add(Arrays.copyOfRange(value, i, i + 2));
      }
      segTs.add(Arrays.copyOfRange(value, value.length - 3, value.length));
    }
    ArrayList<Double> mergeCost = new ArrayList<>();
    for (int i = 0; i < segTs.size() - 1; i++) {
      mergeCost.add(calculateError(segTs.get(i), segTs.get(i + 1)));
    }
    while (Collections.min(mergeCost) < maxError) {
      int index = mergeCost.indexOf(Collections.min(mergeCost));
      segTs.set(index, ArrayUtils.addAll(segTs.get(index), segTs.get(index + 1)));
      segTs.remove(index + 1);
      mergeCost.remove(index);
      if (segTs.size() == 1) {
        break;
      }
      if (index + 1 < segTs.size()) {
        mergeCost.set(index, calculateError(segTs.get(index), segTs.get(index + 1)));
      }
      if (index > 0) {
        mergeCost.set(index - 1, calculateError(segTs.get(index - 1), segTs.get(index)));
      }
    }
    return segTs;
  }

  private static double[] bestLine(
      double maxError, double[] inputDf, double[] w, int startIdx, double upperBound)
      throws Exception {
    double error = 0.0;
    int idx = startIdx + w.length;
    double[] sPrev = w.clone();
    if (idx >= inputDf.length) {
      return sPrev;
    }
    while (error <= maxError) {
      double[] s = ArrayUtils.addAll(sPrev, inputDf[idx]);
      idx++;
      double[] times = new double[s.length];
      for (int i = 0; i < times.length; i++) {
        times[i] = i;
      }
      error = new LinearRegression(times, s).getMAbsE();
      if (error <= maxError) {
        sPrev = s.clone();
      }
      if (sPrev.length > upperBound || idx >= inputDf.length) {
        break;
      }
    }
    return sPrev;
  }

  public static double[] approximatedSegment(double[] inSeq) throws Exception {
    if (inSeq.length <= 2) {
      return inSeq;
    }
    double[] times = new double[inSeq.length];
    for (int i = 0; i < times.length; i++) {
      times[i] = i;
    }
    return new LinearRegression(times, inSeq).getYhead();
  }

  private static List<double[]> swab(double[] inputDf, double maxError, int inWindowSize)
      throws Exception {
    int curNr = 0;
    int wNr = curNr + inWindowSize;
    int totSize = inputDf.length;
    double[] w = Arrays.copyOfRange(inputDf, curNr, wNr);
    int upperBound = 2 * wNr;
    List<double[]> segTs = new ArrayList<>();
    boolean lastRun = false;
    while (true) {
      List<double[]> t = bottomUp(w, maxError);
      segTs.add(t.get(0));
      if (curNr >= totSize || lastRun) {
        if (t.size() > 1) {
          segTs.add(approximatedSegment(t.get(1)));
        }
        break;
      }
      curNr += t.get(0).length - 1;
      wNr = curNr + inWindowSize;
      if (inputDf.length <= wNr) {
        lastRun = true;
        w = Arrays.copyOfRange(inputDf, curNr, inputDf.length);
      } else {
        w = Arrays.copyOfRange(inputDf, curNr, wNr);
      }
      Arrays.sort(w);
      w = bestLine(maxError, inputDf, w, curNr, upperBound);
      if (w.length > upperBound) {
        w = Arrays.copyOfRange(w, 0, upperBound);
      }
    }
    return segTs;
  }

  public static List<double[]> swabAlg(double[] df, double maxError, int windowsize)
      throws Exception {
    return swab(df, maxError, windowsize);
  }
}
