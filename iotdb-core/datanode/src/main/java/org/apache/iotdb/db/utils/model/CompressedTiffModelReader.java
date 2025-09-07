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

  @Override
  public List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    if (startAndEndTimeArray == null || startAndEndTimeArray.isEmpty()) {
      return Collections.emptyList();
    }

    // 为保证返回顺序与输入一致：先把输入解析成条目列表（带原始 index）
    class Req {
      final int idx; // 原始顺序
      final int startPix; // 像元索引（0..total-1）
      final int endPix; // 像元索引（>=startPix）
      int row, col0, col1; // 解析后的行号与列范围

      Req(int idx, int s, int e) {
        this.idx = idx;
        this.startPix = Math.min(s, e);
        this.endPix = Math.max(s, e);
      }
    }
    List<Req> reqs = new ArrayList<>(startAndEndTimeArray.size());
    for (int i = 0; i < startAndEndTimeArray.size(); i++) {
      List<Integer> r = startAndEndTimeArray.get(i);
      if (r == null || r.size() < 2) {
        throw new IllegalArgumentException("Each range must be [start, end].");
      }
      reqs.add(new Req(i, r.get(0), r.get(1)));
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

      // 解析行/列 & 校验仅同一行
      for (Req q : reqs) {
        if (q.startPix < 0 || (long) q.endPix >= total) {
          throw new IndexOutOfBoundsException(
              String.format("Range [%d,%d] out of bounds [0,%d).", q.startPix, q.endPix, total));
        }
        int sRow = q.startPix / width, sCol = q.startPix % width;
        int eRow = q.endPix / width, eCol = q.endPix % width;
        if (sRow != eRow) {
          throw new IllegalArgumentException(
              "Range crosses rows: [" + q.startPix + "," + q.endPix + "]");
        }
        q.row = sRow;
        q.col0 = sCol;
        q.col1 = eCol;
      }

      // NoData 配置
      Double[] nd = new Double[1];
      band.GetNoDataValue(nd);
      final boolean needMapNoData = nd[0] != null && !Double.isNaN(nd[0]);
      final float nodata = needMapNoData ? nd[0].floatValue() : Float.NaN;

      // 行 -> （合并前的列区间列表）
      Map<Integer, List<int[]>> perRow = new LinkedHashMap<>();
      for (Req q : reqs) {
        perRow.computeIfAbsent(q.row, k -> new ArrayList<>()).add(new int[] {q.col0, q.col1});
      }

      // 为每个请求预先分配目标数组，最后按 idx 顺序收集
      float[][] outputs = new float[reqs.size()][];
      for (Req q : reqs) {
        outputs[q.idx] = new float[q.col1 - q.col0 + 1];
      }

      // mergeGap 可按需要调大，如 4/8（允许读取少量“间隙像元”换更少的 ReadRaster 次数）
      final int mergeGap = 0;

      // 行内再建立“原始区间列表”（保持输入顺序），用于把窗口数据拆回去
      Map<Integer, List<Req>> rowReqs = new LinkedHashMap<>();
      for (Req q : reqs) rowReqs.computeIfAbsent(q.row, k -> new ArrayList<>()).add(q);

      // 逐行处理
      for (Map.Entry<Integer, List<int[]>> e : perRow.entrySet()) {
        int row = e.getKey();
        List<int[]> ranges = e.getValue();

        // 合并区间 -> 更少的读取窗口
        List<int[]> merged = mergeRanges(ranges, mergeGap);

        // 按起点排序，便于窗口覆盖
        List<Req> rowList = rowReqs.get(row);
        rowList.sort(Comparator.comparingInt(a -> a.col0));
        int p = 0;

        for (int[] win : merged) {
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

          // 把窗口分发给所有落在其中的原始区间
          while (p < rowList.size()) {
            Req q = rowList.get(p);
            if (q.col1 < c0) {
              p++;
              continue;
            } // 在窗口左侧，跳过
            if (q.col0 > c1) {
              break;
            } // 窗口右侧，进入下一个窗口

            int from = Math.max(q.col0, c0);
            int to = Math.min(q.col1, c1);
            int len = to - from + 1;

            float[] dst = outputs[q.idx];
            System.arraycopy(scratch, from - c0, dst, from - q.col0, len);

            // 注意：若 mergeGap>0，极端情况下一个 req 可能跨两个合并窗口；
            // 这里不 p++，而是仅当整个 req 覆盖完才前移指针
            if (to == q.col1) {
              p++;
            } // 该 req 已经完全覆盖
            else {
              break;
            } // 仍有剩余，等待下一窗口补齐
          }
        }
      }

      // NoData -> NaN
      if (needMapNoData) {
        for (float[] arr : outputs) {
          for (int i = 0; i < arr.length; i++) if (arr[i] == nodata) arr[i] = Float.NaN;
        }
      }

      // 按原始顺序返回
      List<float[]> result = new ArrayList<>(outputs.length);
      for (int i = 0; i < outputs.length; i++) result.add(outputs[i]);
      return result;
    } finally {
      ds.delete();
    }
  }
}
