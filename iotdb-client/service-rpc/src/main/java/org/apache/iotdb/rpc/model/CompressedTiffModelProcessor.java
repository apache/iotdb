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

public class CompressedTiffModelProcessor extends ModelProcessor {
  private static final Driver DRIVER;
  private static final String VIRTUAL_FILE_PATH_PREFIX = "/vsimem";
  private static final String VIRTUAL_FILE_PATH_SUFFIX = ".tif";
  // Specifying compression options
  private static String compressOption = "COMPRESS=LZW";
  // Specifying block x size options
  private static String blockXSize = "BLOCKXSIZE=256";
  // Specifying block y size options
  private static String blockYSize = "BLOCKYSIZE=256";

  static {
    gdal.AllRegister();
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
    // floating point data should use predictor 2 (for difference prediction), and use block storage
    // (recommended for LZW)
    String[] options =
        new String[] {compressOption, "PREDICTOR=2", "TILED=YES", blockXSize, blockYSize};

    Dataset dataset = null;
    try {
      // Create dataset with specified options
      dataset = DRIVER.Create(filePath, width, height, 1, gdalconstConstants.GDT_Float32, options);

      if (dataset == null) {
        throw new RuntimeException("Failed to create dataset: " + gdal.GetLastErrorMsg());
      }

      Band band = dataset.GetRasterBand(1);
      int result = band.WriteRaster(0, 0, width, height, values);
      if (result != gdalconstConstants.CE_None) {
        throw new RuntimeException("Failed to write data to tiff file: " + gdal.GetLastErrorMsg());
      }
      band.FlushCache();
      dataset.FlushCache();
      return VsiGdalNative.vsiGetMemFileBuffer(filePath, true);
    } finally {
      if (dataset != null) {
        dataset.delete();
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
    Dataset dataset = gdal.Open(filePath, gdalconstConstants.GA_ReadOnly);
    if (dataset == null) {
      throw new RuntimeException("Failed to open tiff file: " + gdal.GetLastErrorMsg());
    }
    try {
      Band band = dataset.GetRasterBand(1);
      if (band == null) {
        throw new RuntimeException(
            "Failed to get raster band from dataset" + gdal.GetLastErrorMsg());
      }
      int width = band.getXSize();
      int height = band.getYSize();
      float[] result = new float[width * height];
      band.ReadRaster(0, 0, width, height, gdalconstConstants.GDT_Float32, result);
      return result;
    } finally {
      dataset.delete();
    }
  }

  private String getVirtualFilePath() {
    long tid = Thread.currentThread().getId();
    long timestamp = System.currentTimeMillis();
    return String.format(
        "%s/%d_%d%s", VIRTUAL_FILE_PATH_PREFIX, tid, timestamp, VIRTUAL_FILE_PATH_SUFFIX);
  }
}
