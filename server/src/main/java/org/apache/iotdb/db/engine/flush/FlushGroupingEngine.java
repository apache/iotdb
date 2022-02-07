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

public class FlushGroupingEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(MemTableFlushTask.class);

  public static class ColumnGroup {
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

    public ColumnGroup(
        ArrayList<Integer> columns, int maxGain, ArrayList<BitMap> bitmap, int size, int GroupNum) {
      this.columns = columns;
      this.maxGain = maxGain;
      this.bitmap = bitmap;
      this.size = size;
      this.gains = new ArrayList<>(Collections.nCopies(GroupNum, -1));
      this.updateTimestampInfo();
    }

    public void updateTimestampInfo() {
      int onesCount = 0;
      this.startTimestampIdx = -1;
      this.lastTimestampIdx = -1;
      int bitmapSize = ARRAY_SIZE;
      if (this.bitmap == null) {}

      for (int i = 0; i < this.bitmap.size(); i++) {
        if (this.bitmap.get(i) == null) {
          if (this.startTimestampIdx == -1) {
            this.startTimestampIdx = i * bitmapSize;
          }
          if (i * bitmapSize + bitmapSize - 1 < size) {
            this.lastTimestampIdx = i * bitmapSize + bitmapSize - 1;
          } else {
            if (i * bitmapSize - 1 < size) {
              this.lastTimestampIdx = size - 1;
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
              }
            }
            onesCount += countOnesForByte(bm.getByteArray()[j]);
          }
        }
      }
      this.timeSeriesLength = this.size - onesCount;
    }

    public static ColumnGroup mergeColumnGroup(ColumnGroup g1, ColumnGroup g2) {
      // merge bitmap
      ArrayList<BitMap> newBitmap = new ArrayList<>();
      for (int i = 0; i < g1.bitmap.size(); i++) {
        int bitmapLength = ARRAY_SIZE;
        byte[] bitmap = new byte[bitmapLength / Byte.SIZE + 1];
        for (int j = 0; j < bitmap.length; j++) {
          byte g1byte = 0;
          if (g1.bitmap.get(i) != null) {
            g1byte = g1.bitmap.get(i).getByteArray()[j];
          }
          byte g2byte = 0;
          if (g2.bitmap.get(i) != null) {
            g2byte = g2.bitmap.get(i).getByteArray()[j];
          }
          bitmap[j] = (byte) (g1byte & g2byte);
        }
        newBitmap.add(new BitMap(ARRAY_SIZE, bitmap));
      }
      // merge columns
      ArrayList<Integer> newColumns = (ArrayList<Integer>) g1.columns.clone();
      newColumns.addAll(g2.columns);

      // update posIndex
      return new ColumnGroup(newColumns, -1, newBitmap, g1.size, g1.gains.size() - 1);
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
      ArrayList<ColumnGroup> columnGroups = new ArrayList<>();

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
        columnGroups.add(new ColumnGroup(newGroup, -1, bitmap, size, timeSeriesNumber));
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

        ColumnGroup newGroup =
            ColumnGroup.mergeColumnGroup(columnGroups.get(source), columnGroups.get(target));

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
            ColumnGroup g = columnGroups.get(i);
            ColumnGroup gSource = columnGroups.get(source);
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

  public static int computeOverlap(ArrayList<BitMap> col1, ArrayList<BitMap> col2, int size) {
    int bitmapSize = ARRAY_SIZE;
    int overlaps = 0;
    for (int i = 0; i < col1.size(); i++) {
      byte[] byteCol1 = null;
      byte[] byteCol2 = null;
      if (col1.get(i) != null) {
        byteCol1 = col1.get(i).getByteArray();
      }
      if (col2.get(i) != null) {
        byteCol2 = col2.get(i).getByteArray();
      }
      overlaps += computeOverlapForByte(byteCol1, byteCol2, bitmapSize);
    }
    int repeatOverlap = bitmapSize - (size % bitmapSize);
    if (size % bitmapSize == 0) {
      repeatOverlap = 0;
    }
    return overlaps - repeatOverlap;
  }

  public static int computeOverlapForByte(byte[] col1, byte[] col2, int size) {
    if (col1 == null && col2 == null) {
      return size;
    }
    int onesCount = 0;
    if (col1 == null) {
      for (int i = 0; i < size / Byte.SIZE; i++) {
        onesCount += countOnesForByte(col2[i]);
      }
      return size - onesCount;
    }
    if (col2 == null) {
      for (int i = 0; i < size / Byte.SIZE; i++) {
        onesCount += countOnesForByte(col1[i]);
      }
      return size - onesCount;
    }
    for (int i = 0; i < size / Byte.SIZE; i++) {
      onesCount += countOnesForByte((byte) (col1[i] | col2[i]));
    }
    return size - onesCount;
  }

  public static int computeGain(ColumnGroup g1, ColumnGroup g2, int size) {
    int overlap = computeOverlap(g1.bitmap, g2.bitmap, size);
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
