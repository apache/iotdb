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

package org.apache.iotdb.rpc.model;

import org.gdal.VsiGdalNative;
import org.gdal.gdal.Band;
import org.gdal.gdal.Dataset;
import org.gdal.gdal.Driver;
import org.gdal.gdal.gdal;
import org.gdal.gdalconst.gdalconstConstants;

import java.util.ArrayList;
import java.util.List;

public class CompressedTiffModelProcessor extends ModelProcessor {
  private static final Driver DRIVER;
  private static final String VIRTUAL_FILE_PATH_PREFIX = "/vsimem";
  private static final String VIRTUAL_FILE_PATH_SUFFIX = ".tif";
  // ---- 写入压缩配置（核心配置）----
  private static final String COMPRESS = "ZSTD"; // 备选："DEFLATE" / "LZW"
  private static final int ZSTD_LEVEL = 3; // 仅 ZSTD 有效，可调 1~22
  private static final String PREDICTOR = "2"; // 浮点差分
  private static final int ROWS_PER_STRIP = 12; // 行随机读最佳；若 strip 过多可设 8/16/32

  // 固定哨兵值作为 NoData（兼容面好于直接写 NaN）
  private static final float NODATA_SENTINEL = -3.4028235e38f;

  static {
    gdal.AllRegister();
    gdal.SetConfigOption("GDAL_NUM_THREADS", "ALL_CPUS");
    DRIVER = gdal.GetDriverByName("GTiff");
    if (DRIVER == null) {
      throw new RuntimeException("Failed to get GTiff driver: " + gdal.GetLastErrorMsg());
    }
  }

  @Override
  public byte[] write(float[] values, int width, int height) {
    String virtualFilePath = getVirtualFilePath();
    try {
      return write(virtualFilePath, values, width, height);
    } finally {
      gdal.Unlink(virtualFilePath);
    }
  }

  private byte[] write(String filePath, float[] values, int width, int height) {
    if (values == null || values.length != (long) width * height) {
      throw new IllegalArgumentException("values length must be width*height");
    }

    List<String> opts = new ArrayList<>();
    opts.add("BIGTIFF=IF_SAFER");
    opts.add("TILED=NO"); // 明确 strip 组织
    opts.add("BLOCKYSIZE=" + ROWS_PER_STRIP); // rows-per-strip
    opts.add("PREDICTOR=" + PREDICTOR);
    opts.add("NUM_THREADS=ALL_CPUS");
    if ("ZSTD".equalsIgnoreCase(COMPRESS)) {
      opts.add("COMPRESS=ZSTD");
      opts.add("ZSTD_LEVEL=" + ZSTD_LEVEL);
    } else if ("DEFLATE".equalsIgnoreCase(COMPRESS)) {
      opts.add("COMPRESS=DEFLATE");
    } else if ("LZW".equalsIgnoreCase(COMPRESS)) {
      opts.add("COMPRESS=LZW");
    } else {
      throw new IllegalArgumentException("Unsupported COMPRESS=" + COMPRESS);
    }
    String[] options = opts.toArray(new String[0]);

    Dataset ds = null;
    try {
      ds = DRIVER.Create(filePath, width, height, 1, gdalconstConstants.GDT_Float32, options);
      if (ds == null) {
        throw new RuntimeException("Failed to create dataset: " + gdal.GetLastErrorMsg());
      }
      Band band = ds.GetRasterBand(1);

      // 统一设置 NoData（固定哨兵值）
      band.SetNoDataValue(NODATA_SENTINEL);

      // 顺序写整幅数据（strip 组织下吞吐较好）
      int err = band.WriteRaster(0, 0, width, height, values);
      if (err != gdalconstConstants.CE_None) {
        throw new RuntimeException("Failed to write data: " + gdal.GetLastErrorMsg());
      }

      band.FlushCache();
      ds.FlushCache();
      return VsiGdalNative.vsiGetMemFileBuffer(filePath, true);
    } finally {
      if (ds != null) {
        ds.delete();
      }
    }
  }

  @Override
  public float[] readAll(byte[] fileBytes) {
    String virtualFilePath = getVirtualFilePath();
    try {
      gdal.FileFromMemBuffer(virtualFilePath, fileBytes);
      return readAll(virtualFilePath);
    } finally {
      gdal.Unlink(virtualFilePath);
    }
  }

  @Override
  public float[] readAll(String filePath) {
    Dataset ds = gdal.OpenShared(filePath, gdalconstConstants.GA_ReadOnly);
    if (ds == null) {
      throw new RuntimeException("Failed to open: " + gdal.GetLastErrorMsg());
    }
    try {
      Band band = ds.GetRasterBand(1);
      int w = band.getXSize(), h = band.getYSize();
      float[] out = new float[(int) ((long) w * h)];
      int err = band.ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float32, out);
      if (err != gdalconstConstants.CE_None) {
        throw new RuntimeException("ReadRaster(all) failed: " + gdal.GetLastErrorMsg());
      }

      // NoData -> NaN
      Double[] nd = new Double[1];
      band.GetNoDataValue(nd);
      float nodata = (nd[0] == null || Double.isNaN(nd[0])) ? Float.NaN : nd[0].floatValue();
      if (!Float.isNaN(nodata)) {
        for (int i = 0; i < out.length; i++) {
          if (out[i] == nodata) {
            out[i] = Float.NaN;
          }
        }
      }
      return out;
    } finally {
      ds.delete();
    }
  }

  private String getVirtualFilePath() {
    long tid = Thread.currentThread().getId();
    long timestamp = System.currentTimeMillis();
    return String.format(
        "%s/%d_%d%s", VIRTUAL_FILE_PATH_PREFIX, tid, timestamp, VIRTUAL_FILE_PATH_SUFFIX);
  }
}
