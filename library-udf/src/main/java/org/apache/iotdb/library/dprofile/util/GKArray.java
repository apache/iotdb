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

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;

import java.io.Serializable;
import java.util.Comparator;

/** Util for UDAFPercentile */
public class GKArray {

  private final double rankAccuracy;
  private MutableList<Tuple> entries;
  private final double[] incoming;
  private int incomingIndex;
  private long compressedCount;
  private double minValue;

  public GKArray(double rankAccuracy) {
    this.rankAccuracy = rankAccuracy;
    this.entries = Lists.mutable.empty();
    this.incoming = new double[(int) (1 / rankAccuracy) + 1];
    this.incomingIndex = 0;
    this.minValue = Double.MAX_VALUE;
    this.compressedCount = 0;
  }

  public void insert(double value) {
    if (Double.isFinite(value)) {
      incoming[incomingIndex++] = value;
      if (incomingIndex == incoming.length) {
        compress();
      }
    }
  }

  public boolean isEmpty() {
    return entries.isEmpty() && incomingIndex == 0;
  }

  public double query(double phi) {
    if (isEmpty()) {
      throw new ArithmeticException();
    }
    compressIfNecessary();

    final long rank = (long) (phi * (compressedCount - 1)) + 1;
    final long spread = (long) (rankAccuracy * (compressedCount - 1));
    long gSum = 0;
    int i;
    for (i = 0; i < entries.size(); i++) {
      gSum += entries.get(i).g;
      if (gSum + entries.get(i).delta > rank + spread) {
        break;
      }
    }

    if (i == 0) {
      return minValue;
    } else {
      return entries.get(i - 1).v;
    }
  }

  private void compressIfNecessary() {
    if (incomingIndex > 0) {
      compress();
    }
  }

  private void compress() {
    compress(Lists.mutable.empty());
  }

  private void compress(MutableList<Tuple> additionalEntries) {

    for (int i = 0; i < incomingIndex; i++) {
      additionalEntries.add(new Tuple(incoming[i], 1, 0));
    }
    additionalEntries.sort(Comparator.comparingDouble(e -> e.v));

    compressedCount += incomingIndex;
    if (!additionalEntries.isEmpty()) {
      minValue = Math.min(minValue, additionalEntries.get(0).v);
    }

    final long removalThreshold = 2 * (long) (rankAccuracy * (compressedCount - 1));
    final MutableList<Tuple> mergedEntries =
        Lists.mutable.ofInitialCapacity(entries.size() + additionalEntries.size() / 3);

    int i = 0, j = 0;
    while (i < additionalEntries.size() || j < entries.size()) {
      if (i == additionalEntries.size()) {
        if (j + 1 < entries.size()
            && entries.get(j).g + entries.get(j + 1).g + entries.get(j + 1).delta
                <= removalThreshold) {
          // Removable from sketch.
          entries.get(j + 1).g += entries.get(j).g;
        } else {
          mergedEntries.add(entries.get(j));
        }

        j++;

      } else if (j == entries.size()) {
        // Done with sketch; now only considering incoming.
        if (i + 1 < additionalEntries.size()
            && additionalEntries.get(i).g
                    + additionalEntries.get(i + 1).g
                    + additionalEntries.get(i + 1).delta
                <= removalThreshold) {
          // Removable from incoming.
          additionalEntries.get(i + 1).g += additionalEntries.get(i).g;
        } else {
          mergedEntries.add(additionalEntries.get(i));
        }

        i++;

      } else if (additionalEntries.get(i).v < entries.get(j).v) {
        if (additionalEntries.get(i).g + entries.get(j).g + entries.get(j).delta
            <= removalThreshold) {
          entries.get(j).g += additionalEntries.get(i).g;
        } else {
          additionalEntries.get(i).delta =
              entries.get(j).g + entries.get(j).delta - additionalEntries.get(i).g;
          mergedEntries.add(additionalEntries.get(i));
        }

        i++;

      } else {
        if (j + 1 < entries.size()
            && entries.get(j).g + entries.get(j + 1).g + entries.get(j + 1).delta
                <= removalThreshold) {
          // Removable from sketch.
          entries.get(j + 1).g += entries.get(j).g;
        } else {
          mergedEntries.add(entries.get(j));
        }

        j++;
      }
    }
    entries = mergedEntries;
    incomingIndex = 0;
  }

  static class Tuple implements Serializable {

    private final double v;
    private long g;
    private long delta;

    private Tuple(double v, long g, long delta) {
      this.v = v;
      this.g = g;
      this.delta = delta;
    }
  }
}
