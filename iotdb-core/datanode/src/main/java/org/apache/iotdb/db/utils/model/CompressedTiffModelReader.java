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
import java.util.List;

public class CompressedTiffModelReader extends ModelReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompressedTiffModelReader.class);

  static {
    gdal.AllRegister();
    int cacheMax = gdal.GetCacheMax();
    LOGGER.info("GDAL Cache Max: {}", cacheMax);
    gdal.SetCacheMax(cacheMax * 2);
  }

  @Override
  public List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    Dataset dataset = gdal.Open(filePath, gdalconstConstants.GA_ReadOnly);
    if (dataset == null) {
      LOGGER.error("Failed to open tiff file: {}", gdal.GetLastErrorMsg());
      throw new RuntimeException("Failed to open tiff file: " + gdal.GetLastErrorMsg());
    }
    try {
      Band band = dataset.GetRasterBand(1);
      if (band == null) {
        LOGGER.error("Failed to get raster band from dataset: {}", gdal.GetLastErrorMsg());
        throw new RuntimeException(
            "Failed to get raster band from dataset" + gdal.GetLastErrorMsg());
      }
      int width = band.getXSize();
      List<float[]> result = new ArrayList<>();
      for (List<Integer> startAndEndTime : startAndEndTimeArray) {
        int xIndex = startAndEndTime.get(0);
        int yIndex = startAndEndTime.get(1);
        float[] tmp = new float[yIndex - xIndex + 1];
        int xOff = xIndex % width;
        int yOff = xIndex / width;
        int xSize = yIndex - xIndex + 1;
        int ySize = 1;
        band.ReadRaster(xOff, yOff, xSize, ySize, gdalconstConstants.GDT_Float32, tmp);
        result.add(tmp);
      }
      return result;
    } finally {
      dataset.delete();
    }
  }
}
