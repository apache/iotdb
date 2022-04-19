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

package org.apache.iotdb.db.engine.flush;

import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.rescon.PrimitiveArrayManager.ARRAY_SIZE;

public class ApproximateFlushGroupingEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);

  public static class ColumnGroupAppr {
    public ArrayList<Integer> columns;
    public int maxGain = -1;
    public int maxGainPosition = -1;
    public long lastTimestampIdx;
    public long startTimestampIdx;
    public int timeSeriesLength; // number of exist timestamps, i.e., zeros in bitmap
    public long interval;
    public ArrayList<BitMap> bitmap;
    public int size; // total length of the bitmap
    public ArrayList<Integer> gains;
    public ArrayList<Feature> features;
    public ArrayList<Long> rangeMap;
    public ArrayList<ArrayList<Integer>> rangeMapCols;

    public ColumnGroupAppr(
        ArrayList<Integer> columns,
        int maxGain,
        ArrayList<BitMap> bitmap,
        int size,
        int GroupNum,
        AlignedTVList dataList) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.bitmap = bitmap;
      this.size = size;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.updateTimestampInfo(dataList);
    }

    public ColumnGroupAppr(
        ArrayList<Integer> columns,
        int maxGain,
        int timeSeriesLength,
        int GroupNum,
        ArrayList<Feature> features,
        ArrayList<Long> rangeMap,
        ArrayList<ArrayList<Integer>> rangeMapCols) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.timeSeriesLength = timeSeriesLength;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.features = features;
      this.rangeMap = rangeMap;
      this.rangeMapCols = rangeMapCols;
    }

    public void updateTimestampInfo(AlignedTVList dataList) {
      int onesCount = 0;
      this.startTimestampIdx = -1;
      this.lastTimestampIdx = -1;
      ArrayList<Long> timestamps = new ArrayList<>();
      int bitmapSize = ARRAY_SIZE;
      int intervalTimes = 1000;
      if (this.bitmap == null) {
        for (int i = 0; i < size; i++) {
          timestamps.add(dataList.getTime(i) / intervalTimes);
        }
      } else {
        for (int i = 0; i < this.bitmap.size(); i++) {
          if (this.bitmap.get(i) == null) {
            if (this.startTimestampIdx == -1) {
              this.startTimestampIdx = i * bitmapSize;
            }
            if (i * bitmapSize + bitmapSize - 1 < size) {
              this.lastTimestampIdx = i * bitmapSize + bitmapSize - 1;
              for (int k = i * bitmapSize; k <= i * bitmapSize + bitmapSize - 1; k++) {
                timestamps.add(dataList.getTime(k) / intervalTimes);
              }
            } else {
              if (i * bitmapSize - 1 < size) {
                this.lastTimestampIdx = size - 1;
                for (int k = i * bitmapSize; k <= size - 1; k++) {
                  timestamps.add(dataList.getTime(k) / intervalTimes);
                }
              }
            }
          } else {
            BitMap bm = this.bitmap.get(i);
            for (int j = 0; j < bm.getSize() / Byte.SIZE; j++) {
              int onesNumber = countOnesForByte(bm.getByteArray()[j]);
              if (this.startTimestampIdx == -1 && onesNumber != 8) {
                this.startTimestampIdx = i * bitmapSize + j * 8 + firstZero(bm.getByteArray()[j]);
              }
              if (onesNumber != 8) {
                if (i * bitmapSize + j * 8 + lastZero(bm.getByteArray()[j]) < size) {
                  this.lastTimestampIdx = i * bitmapSize + j * 8 + lastZero(bm.getByteArray()[j]);
                  for (int k = i * bitmapSize + j * 8 + firstZero(bm.getByteArray()[j]);
                      (k < size) && (k <= i * bitmapSize + j * 8 + lastZero(bm.getByteArray()[j]));
                      k++) {
                    if (!bm.isMarked(k - i * bitmapSize)) {
                      timestamps.add(dataList.getTime(k) / intervalTimes);
                    }
                  }
                }
              }
              onesCount += countOnesForByte(bm.getByteArray()[j]);
            }
          }
        }
      }
      int intervalGran = 1;
      long intervalSum = 0;
      int intervalCount = 0;
      int intervalMax = 1000;

      for (int i = 1; i < timestamps.size(); i++) {
        long interval_ = timestamps.get(i) - timestamps.get(i - 1);
        if (interval_ < intervalMax) {
          intervalSum += interval_;
          intervalCount++;
        }
      }
      long interval = intervalSum / intervalCount;
      interval = interval / intervalGran * intervalGran;

      // compute n
      this.timeSeriesLength = this.size - onesCount;
      // compute start
      long start = timestamps.get(0);
      double sigma = 0;
      double sigmaSum = 0;
      ArrayList<Double> sigmaList = new ArrayList<>();
      long offset = 0;
      for (int i = 0; i < timestamps.size(); i++) {
        sigma = Math.abs(timestamps.get(i) - start - i * interval - offset);
        if (sigma > 10 * interval) {
          sigma = Math.abs(timestamps.get(i) - start - i * interval) % interval;
          offset = Math.abs(timestamps.get(i) - start - i * interval) / interval * interval;
        }
        sigmaSum += sigma;
      }
      sigma = sigmaSum / timestamps.size();

      this.features = new ArrayList<>();
      this.features.add(new Feature(interval, this.timeSeriesLength, start, sigma));

      this.rangeMap = new ArrayList<>();
      this.rangeMapCols = new ArrayList<>();
      rangeMap.add(start);
      rangeMap.add(start + interval * (this.timeSeriesLength - 1));
      ArrayList<Integer> overlapGroup = new ArrayList<>();
      overlapGroup.add(this.columns.get(0));
      rangeMapCols.add(overlapGroup);
    }

    public static ColumnGroupAppr mergeColumnGroup(ColumnGroupAppr g1, ColumnGroupAppr g2) {
      // merge bitmap
      ArrayList<BitMap> newBitmap = new ArrayList<>();

      // merge columns, features;

      ArrayList<Integer> newColumns = (ArrayList<Integer>) g1.columns.clone();
      newColumns.addAll(g2.columns);
      ArrayList<Feature> features = (ArrayList<Feature>) g1.features.clone();
      features.addAll(g2.features);

      int overlap = estimateOverlap(g1, g2);
      int timeSeriesLength = g1.timeSeriesLength + g2.timeSeriesLength - overlap;
      int GroupNum = g1.gains.size() - 1;

      // merge rangeMap
      ArrayList<Long> map1;
      ArrayList<Long> map2;
      ArrayList<ArrayList<Integer>> mapCols1;
      ArrayList<ArrayList<Integer>> mapCols2;
      ArrayList<Long> newMap = new ArrayList<>();
      ArrayList<ArrayList<Integer>> newMapCols = new ArrayList<>();

      if (g1.rangeMap.get(0) < g2.rangeMap.get(0)) {
        map1 = g1.rangeMap;
        map2 = g2.rangeMap;
        mapCols1 = g1.rangeMapCols;
        mapCols2 = g2.rangeMapCols;
      } else {
        map1 = g2.rangeMap;
        map2 = g1.rangeMap;
        mapCols1 = g2.rangeMapCols;
        mapCols2 = g1.rangeMapCols;
      }

      int i = 0;
      int j = 0;

      int lastPoint = map1.get(0).equals(map2.get(0)) ? 3 : 1; // 1: map1, 2:map2, 3: equal
      newMap.add(map1.get(i));

      while (true) {
        if (lastPoint == 1) {
          i += 1;
          if (i >= map1.size()) {
            break;
          }
          if (map1.get(i) < map2.get(j)) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
          } else if (map1.get(i).equals(map2.get(j))) {
            lastPoint = 3;
            newMap.add(map1.get(i));
            if (j == 0) {
              newMapCols.add(mapCols1.get(i - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
              tmpCols.addAll(mapCols2.get(j - 1));
              newMapCols.add(tmpCols);
            }
          } else {
            lastPoint = 2;
            newMap.add(map2.get(j));
            if (j == 0) {
              newMapCols.add(mapCols1.get(i - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
              tmpCols.addAll(mapCols2.get(j - 1));
              newMapCols.add(tmpCols);
            }
          }

        } else if (lastPoint == 2) {
          j += 1;
          if (j >= map2.size()) {
            break;
          }
          if (map2.get(j) < map1.get(i)) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
          } else if (map1.get(i).equals(map2.get(j))) {
            lastPoint = 3;
            newMap.add(map2.get(j));
            if (i == 0) {
              newMapCols.add(mapCols2.get(j - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols2.get(j - 1).clone();
              tmpCols.addAll(mapCols1.get(i - 1));
              newMapCols.add(tmpCols);
            }
          } else {
            lastPoint = 1;
            newMap.add(map1.get(i));
            if (i == 0) {
              newMapCols.add(mapCols2.get(j - 1));
            } else {
              ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols2.get(j - 1).clone();
              tmpCols.addAll(mapCols1.get(i - 1));
              newMapCols.add(tmpCols);
            }
          }
        } else {
          // lastPoint == 3
          i += 1;
          j += 1;
          if ((i >= map1.size()) || (j >= map2.size())) {
            break;
          }
          if (map1.get(i) < map2.get(j)) {
            lastPoint = 1;
            newMap.add(map1.get(i));
          } else if (map1.get(i) == map2.get(j)) {
            newMap.add(map1.get(i));
          } else {
            lastPoint = 3;
            newMap.add(map2.get(j));
          }
          ArrayList<Integer> tmpCols = (ArrayList<Integer>) mapCols1.get(i - 1).clone();
          tmpCols.addAll(mapCols2.get(j - 1));
          newMapCols.add(tmpCols);
        }
      }

      if (i >= map1.size()) {
        if (lastPoint == 1) {
          while (j < map2.size()) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
            j += 1;
          }
        } else {
          j += 1;
          while (j < map2.size()) {
            newMap.add(map2.get(j));
            newMapCols.add(mapCols2.get(j - 1));
            j += 1;
          }
        }
      } else if (j >= map2.size()) {
        if (lastPoint == 2) {
          while (i < map1.size()) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
            i += 1;
          }
        } else {
          i += 1;
          while (i < map1.size()) {
            newMap.add(map1.get(i));
            newMapCols.add(mapCols1.get(i - 1));
            i += 1;
          }
        }
      }

      return new ColumnGroupAppr(
          newColumns, -1, timeSeriesLength, GroupNum, features, newMap, newMapCols);
    }
  }

  /** autoaligned grouping method * */
  public static void grouping(AlignedTVList dataList, List<IMeasurementSchema> schemaList) {
    try {
      if (dataList.getBitMaps() == null) {
        return;
      }

      int timeSeriesNumber = dataList.getValues().size();
      int size = dataList.rowCount();
      ArrayList<ColumnGroupAppr> columnGroups = new ArrayList<>();

      if (timeSeriesNumber == 0) {
        return;
      }
      if (timeSeriesNumber == 1) {
        return;
      }

      // init posMap, maxGain, groupingResult
      for (int i = 0; i < timeSeriesNumber; i++) {
        ArrayList<Integer> newGroup = new ArrayList<>();
        newGroup.add(i);
        ArrayList<BitMap> bitmap = (ArrayList<BitMap>) dataList.getBitMaps().get(i);
        if (bitmap == null) {
          bitmap = new ArrayList<>();
          for (int j = 0; j < dataList.getBitMaps().size(); j++) {
            if (dataList.getBitMaps().get(j) != null) {
              int sizeBitmap = dataList.getBitMaps().get(j).size();
              for (int k = 0; k < sizeBitmap; k++) {
                bitmap.add(null);
              }
            }
          }
        }
        columnGroups.add(
            new ColumnGroupAppr(newGroup, -1, bitmap, size, timeSeriesNumber, dataList));
      }

      // init gain matrix
      for (int i = 0; i < timeSeriesNumber; i++) {
        for (int j = i + 1; j < timeSeriesNumber; j++) {
          int gain = computeGain(columnGroups.get(i), columnGroups.get(j), size);
          columnGroups.get(i).gains.set(j, gain);
          columnGroups.get(j).gains.set(i, gain);
          if (columnGroups.get(i).maxGain < gain) {
            columnGroups.get(i).maxGain = gain;
            columnGroups.get(i).maxGainPosition = j;
          }
          if (columnGroups.get(j).maxGain < gain) {
            columnGroups.get(j).maxGain = gain;
            columnGroups.get(j).maxGainPosition = i;
          }
        }
      }

      while (true) {
        /** merge * */

        // find the main gain
        int maxGainOfAll = -1;
        int maxGainOfAllPos = -1;
        for (int i = 0; i < columnGroups.size(); i++) {
          if (columnGroups.get(i).maxGain > maxGainOfAll) {
            maxGainOfAll = columnGroups.get(i).maxGain;
            maxGainOfAllPos = i;
          }
        }
        if (maxGainOfAll <= 0) {
          break;
        }

        // merge the group, create a new group
        int source = maxGainOfAllPos;
        int target = columnGroups.get(source).maxGainPosition;

        ColumnGroupAppr newGroup =
            ColumnGroupAppr.mergeColumnGroup(columnGroups.get(source), columnGroups.get(target));

        // remove the old groups
        columnGroups.remove(target);
        columnGroups.remove(source);

        // load target into source
        columnGroups.add(source, newGroup);

        /** update * */

        // update gains
        // remove the target, and update the source
        for (int i = 0; i < columnGroups.size(); i++) {
          if (i != source) {
            ColumnGroupAppr g = columnGroups.get(i);
            ColumnGroupAppr gSource = columnGroups.get(source);
            g.gains.remove(target);
            int gain = computeGain(g, gSource, size);
            g.gains.set(source, gain);
            // update the maxgain in i
            if ((g.maxGainPosition != source) && (g.maxGainPosition != target)) {
              if (gain > g.maxGain) {
                g.maxGain = gain;
                g.maxGainPosition = source;
              } else {
                if (target < g.maxGainPosition) {
                  g.maxGainPosition -= 1;
                }
              }
            } else {
              g.maxGain = gain;
              g.maxGainPosition = source;
              for (int j = 0; j < g.gains.size(); j++) {
                if (g.gains.get(j) > g.maxGain) {
                  g.maxGain = g.gains.get(j);
                  g.maxGainPosition = j;
                }
              }
            }
            // update the maxgain and data in source
            gSource.gains.set(i, gain);
            if (gain > gSource.maxGain) {
              gSource.maxGain = gain;
              gSource.maxGainPosition = i;
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
  }

  public static double computeProbability(Feature f1, Feature f2) {
    int lambda = 3;
    int tau = 10;
    double prob = 0;
    long lcmInterval = lcm(f1.epsilon, f2.epsilon);
    long scenarios;
    if (f1.epsilon > f2.epsilon) {
      scenarios = lcmInterval / f1.epsilon;
      for (int i = 0; i < scenarios; i++) {
        int delta =
            ((int) (f1.epsilon - f2.epsilon) * i + (int) f1.start - (int) f2.start)
                % (int) f2.epsilon;
        if (f1.sigma == 0 && f2.sigma == 0) {
          if (delta == 0) {
            prob += 1;
          } else {
            prob += 0;
          }
        } else {
          double z1 =
              ((lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          double z2 =
              (-(lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          prob += NormSDist(z1) - NormSDist(z2);
        }
      }
    } else {
      scenarios = lcmInterval / f2.epsilon;
      for (int i = 0; i < scenarios; i++) {
        int delta =
            ((int) (f2.epsilon - f1.epsilon) * i + (int) f2.start - (int) f1.start)
                % (int) f1.epsilon;
        if (f1.sigma == 0 && f2.sigma == 0) {
          if (delta == 0) {
            prob += 1;
          } else {
            prob += 0;
          }
        } else {
          double z1 =
              ((lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          double z2 =
              (-(lambda * tau) - delta) / Math.sqrt(f1.sigma * f1.sigma + f2.sigma * f2.sigma);
          prob += NormSDist(z1) - NormSDist(z2);
        }
      }
    }
    prob /= scenarios;
    return prob;
  }

  public static int estimateOverlap(ColumnGroupAppr g1, ColumnGroupAppr g2) {
    ArrayList<Double> overlapList = new ArrayList<>();

    for (int i = 0; i < g2.columns.size(); i++) {
      Feature f2 = g2.features.get(i);
      long start2 = f2.start;
      long end2 = f2.start + f2.epsilon * (f2.n - 1);
      double overlapTmp = 0;

      int t = 0;

      while ((t < g1.rangeMap.size() - 1) && (g1.rangeMap.get(t) < start2)) {
        t += 1;
      }
      if (t == g1.rangeMap.size() - 1) {
        overlapList.add(0.0);
        continue;
      } else {
        if ((t > 0) && (g1.rangeMap.get(t - 1) < start2)) {
          int n2Tmp = (int) ((start2 - g1.rangeMap.get(t - 1)) / f2.epsilon);
          double prob = 1;
          for (int col : g1.rangeMapCols.get(t - 1)) {
            for (int k = 0; k < g1.columns.size(); k++) {
              if (g1.columns.get(k) == col) {
                Feature f1 = g1.features.get(k);
                prob *= (1 - computeProbability(f1, f2));
                break;
              }
            }
          }
          prob -= 1;
          overlapTmp += prob * n2Tmp;
        }
      }

      if (g1.rangeMap.get(t) >= start2) {
        int j = t;
        while ((j < g1.rangeMap.size() - 1) && (g1.rangeMap.get(j) < end2)) {
          j += 1;
          int n2Tmp;
          if (g1.rangeMap.get(j) < end2) {
            n2Tmp = (int) ((g1.rangeMap.get(j) - g1.rangeMap.get(j - 1)) / f2.epsilon);
          } else {
            n2Tmp = (int) ((end2 - g1.rangeMap.get(j - 1)) / f2.epsilon);
          }
          double prob = 1;
          for (int col : g1.rangeMapCols.get(j - 1)) {
            for (int k = 0; k < g1.columns.size(); k++) {
              if (g1.columns.get(k) == col) {
                Feature f1 = g1.features.get(k);
                prob *= (1 - computeProbability(f1, f2));
                break;
              }
            }
          }
          prob = 1 - prob;
          overlapTmp += prob * n2Tmp;
        }
      }
      overlapList.add(overlapTmp);
    }

    double sumOverlap = 0;
    for (double overlap : overlapList) {
      sumOverlap += overlap;
    }
    int sumN = 0;
    for (Feature f : g2.features) {
      sumN += f.n;
    }

    int overlapEstimation = (int) ((sumOverlap / sumN) * g2.timeSeriesLength);
    return overlapEstimation;
  }

  public static int computeGain(ColumnGroupAppr g1, ColumnGroupAppr g2, int size) {
    int overlap = estimateOverlap(g1, g2);
    //    int overlap = computeOverlap(g1.bitmap, g2.bitmap, size);
    if (g1.columns.size() == 1 && g2.columns.size() == 1) {
      // col - col
      int gain = overlap * Long.SIZE - 2 * (g1.timeSeriesLength + g2.timeSeriesLength - overlap);
      return gain;
    }
    if (g1.columns.size() == 1) {
      // col - group
      int m_s_a = g1.timeSeriesLength;
      int n_g_a = g2.columns.size();
      int m_g_a = g2.timeSeriesLength;
      int gain = overlap * Long.SIZE + n_g_a * m_g_a - (n_g_a + 1) * (m_s_a + m_g_a - overlap);
      return gain;
    }
    if (g2.columns.size() == 1) {
      // group - col
      int m_s_a = g2.timeSeriesLength;
      int n_g_a = g1.columns.size();
      int m_g_a = g1.timeSeriesLength;
      int gain = overlap * Long.SIZE + n_g_a * m_g_a - (n_g_a + 1) * (m_s_a + m_g_a - overlap);
      return gain;
    }

    // group - group
    int n_g_a = g1.columns.size();
    int m_g_a = g1.timeSeriesLength;
    int n_g_b = g2.columns.size();
    int m_g_b = g2.timeSeriesLength;

    int gain = overlap * Long.SIZE + (n_g_a + n_g_b) * overlap - n_g_a * m_g_b - n_g_b * m_g_a;
    return gain;
  }

  /** belows are some util functions* */
  public static class Feature {
    public long epsilon;
    public int n;
    public long start;
    public double sigma;

    public Feature(long epsilon_, int n_, long start_, double sigma_) {
      this.epsilon = epsilon_;
      this.n = n_;
      this.start = start_;
      this.sigma = sigma_;
    }
  }

  public static double NormSDist(double z) {
    // compute approximate normal distribution cdf F(z)
    if (z > 6) return 1;
    if (z < -6) return 0;
    double gamma = 0.231641900,
        a1 = 0.319381530,
        a2 = -0.356563782,
        a3 = 1.781477973,
        a4 = -1.821255978,
        a5 = 1.330274429;
    double x = Math.abs(z);
    double t = 1 / (1 + gamma * x);
    double n =
        1
            - (1 / (Math.sqrt(2 * Math.PI)) * Math.exp(-z * z / 2))
                * (a1 * t
                    + a2 * Math.pow(t, 2)
                    + a3 * Math.pow(t, 3)
                    + a4 * Math.pow(t, 4)
                    + a5 * Math.pow(t, 5));
    if (z < 0) return 1.0 - n;
    return n;
  }

  public static long lcm(long number1, long number2) {
    if (number1 == 0 || number2 == 0) {
      return 0;
    }
    long absNumber1 = Math.abs(number1);
    long absNumber2 = Math.abs(number2);
    long absHigherNumber = Math.max(absNumber1, absNumber2);
    long absLowerNumber = Math.min(absNumber1, absNumber2);
    long lcm = absHigherNumber;
    while (lcm % absLowerNumber != 0) {
      lcm += absHigherNumber;
    }
    return lcm;
  }

  public static int countOnesForByte(byte x) {
    return ((x >> 7) & 1)
        + ((x >> 6) & 1)
        + ((x >> 5) & 1)
        + ((x >> 4) & 1)
        + ((x >> 3) & 1)
        + ((x >> 2) & 1)
        + ((x >> 1) & 1)
        + (x & 1);
  }

  public static int firstZero(byte x) {
    for (int i = 0; i <= 7; i++) {
      if (((x >> i) & 1) == 0) {
        return i;
      }
    }
    return -1;
  }

  public static int lastZero(byte x) {
    for (int i = 7; i >= 0; i--) {
      if (((x >> i) & 1) == 0) {
        return i;
      }
    }
    return -1;
  }
}
