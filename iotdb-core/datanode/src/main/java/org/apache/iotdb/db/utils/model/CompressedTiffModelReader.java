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

package org.apache.iotdb.db.utils.model;

import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CompressedTiffModelReader extends ModelReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedTiffModelReader.class);

  static {
    gdal.AllRegister();
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS");
  }

  // 线程本地复用的读缓冲，避免频繁分配
  private static final ThreadLocal<float[]> TL_SCRATCH =
      ThreadLocal.withInitial(() -> new float[0]);

  private static float[] ensureCapacity(float[] buf, int need) {
    if (buf.length >= need) return buf;
    int cap = Math.max(buf.length * 2, need);
    float[] n = new float[cap];
    TL_SCRATCH.set(n);
    return n;
  }

  /** 合并相邻/重叠区间；mergeGap=0 表示仅相邻或重叠才合并 */
  private static List<int[]> mergeRanges(List<int[]> ranges, int mergeGap) {
    if (ranges.isEmpty()) return ranges;
    ranges.sort(Comparator.comparingInt(a -> a[0]));
    List<int[]> out = new ArrayList<>();
    int s = ranges.get(0)[0], e = ranges.get(0)[1];
    for (int i = 1; i < ranges.size(); i++) {
      int ns = ranges.get(i)[0], ne = ranges.get(i)[1];
      if (ns <= e + 1 + mergeGap) {
        e = Math.max(e, ne);
      } else {
        out.add(new int[] {s, e});
        s = ns;
        e = ne;
      }
    }
    out.add(new int[] {s, e});
    return out;
  }

  static class SearchReq {
    final int idx; // 原始顺序
    final int startPix; // 像元索引
    final int endPix;
    final int row;
    final int col0, col1;

    SearchReq(int idx, int startPix, int endPix, int row, int col0, int col1) {
      this.idx = idx;
      this.startPix = startPix;
      this.endPix = endPix;
      this.row = row;
      this.col0 = col0;
      this.col1 = col1;
    }
  }

  @Override
  public List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    if (startAndEndTimeArray == null || startAndEndTimeArray.isEmpty()) {
      return Collections.emptyList();
    }

    Dataset ds = gdal.OpenShared(filePath, gdalconstConstants.GA_ReadOnly);
    if (ds == null) {
      throw new RuntimeException("Failed to open: " + gdal.GetLastErrorMsg());
    }

    try {
      Band band = ds.GetRasterBand(1);
      final int width = band.getXSize();
      final int height = band.getYSize();
      final long total = (long) width * (long) height;

      // ---- step1: 解析输入，允许跨行 ----
      List<SearchReq> reqs = new ArrayList<>();
      for (int i = 0; i < startAndEndTimeArray.size(); i++) {
        List<Integer> r = startAndEndTimeArray.get(i);
        if (r == null || r.size() < 2) {
          throw new IllegalArgumentException("Each range must be [start, end].");
        }
        int sPix = Math.min(r.get(0), r.get(1));
        int ePix = Math.max(r.get(0), r.get(1));
        if (sPix < 0 || ePix >= total) {
          throw new IndexOutOfBoundsException(
              String.format("Range [%d,%d] out of bounds [0,%d).", sPix, ePix, total));
        }
        int sRow = sPix / width, sCol = sPix % width;
        int eRow = ePix / width, eCol = ePix % width;

        if (sRow == eRow) {
          // 单行：直接保留
          reqs.add(new SearchReq(i, sPix, ePix, sRow, sCol, eCol));
        } else {
          // 跨行：拆分
          // 首行片段
          reqs.add(new SearchReq(i, sPix, (sRow + 1) * width - 1, sRow, sCol, width - 1));
          // 中间整行
          for (int row = sRow + 1; row < eRow; row++) {
            reqs.add(new SearchReq(i, row * width, (row + 1) * width - 1, row, 0, width - 1));
          }
          // 末行片段
          reqs.add(new SearchReq(i, eRow * width, ePix, eRow, 0, eCol));
        }
      }

      // ---- step2: NoData 配置 ----
      Double[] nd = new Double[1];
      band.GetNoDataValue(nd);
      final boolean needMapNoData = nd[0] != null && !Double.isNaN(nd[0]);
      final float nodata = needMapNoData ? nd[0].floatValue() : Float.NaN;

      // ---- step3: 行 -> 区间列表 ----
      Map<Integer, List<int[]>> perRow = new LinkedHashMap<>();
      Map<Integer, List<SearchReq>> rowReqs = new LinkedHashMap<>();
      for (SearchReq q : reqs) {
        perRow.computeIfAbsent(q.row, k -> new ArrayList<>()).add(new int[] {q.col0, q.col1});
        rowReqs.computeIfAbsent(q.row, k -> new ArrayList<>()).add(q);
      }

      // 每个原始 idx -> 拼接结果
      Map<Integer, List<float[]>> grouped = new HashMap<>();

      // ---- step4: 按行读取，保留原优化 ----
      for (Map.Entry<Integer, List<int[]>> e : perRow.entrySet()) {
        int row = e.getKey();
        List<int[]> ranges = mergeRanges(e.getValue(), 0);

        List<SearchReq> rowList = rowReqs.get(row);
        rowList.sort(Comparator.comparingInt(a -> a.col0));
        int p = 0;

        for (int[] win : ranges) {
          int c0 = win[0], c1 = win[1];
          int winLen = c1 - c0 + 1;

          float[] scratch = ensureCapacity(TL_SCRATCH.get(), winLen);
          int err = band.ReadRaster(c0, row, winLen, 1, gdalconstConstants.GDT_Float32, scratch);
          if (err != gdalconstConstants.CE_None) {
            throw new RuntimeException(
                "ReadRaster(row="
                    + row
                    + ", "
                    + c0
                    + ":"
                    + c1
                    + ") failed: "
                    + gdal.GetLastErrorMsg());
          }

          while (p < rowList.size()) {
            SearchReq q = rowList.get(p);
            if (q.col1 < c0) {
              p++;
              continue;
            }
            if (q.col0 > c1) break;

            int from = Math.max(q.col0, c0);
            int to = Math.min(q.col1, c1);
            int len = to - from + 1;
            float[] seg = new float[len];
            System.arraycopy(scratch, from - c0, seg, 0, len);

            grouped.computeIfAbsent(q.idx, k -> new ArrayList<>()).add(seg);

            if (to == q.col1) {
              p++;
            } else break;
          }
        }
      }

      // ---- step5: 拼接 & 替换 NoData ----
      List<float[]> result = new ArrayList<>(startAndEndTimeArray.size());
      for (int i = 0; i < startAndEndTimeArray.size(); i++) {
        List<float[]> parts = grouped.get(i);
        if (parts == null) {
          result.add(new float[0]);
          continue;
        }
        int totalLen = parts.stream().mapToInt(a -> a.length).sum();
        float[] merged = new float[totalLen];
        int pos = 0;
        for (float[] seg : parts) {
          System.arraycopy(seg, 0, merged, pos, seg.length);
          pos += seg.length;
        }
        if (needMapNoData) {
          for (int j = 0; j < merged.length; j++) if (merged[j] == nodata) merged[j] = Float.NaN;
        }
        result.add(merged);
      }
      return result;
    } finally {
      ds.delete();
    }
  }
}
