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

import ij.ImagePlus;
import ij.io.Opener;
import ij.process.FloatProcessor;
import ij.process.ImageProcessor;

import java.util.ArrayList;
import java.util.List;

public class UnCompressedTiffModelReader extends ModelReader {

  @Override
  public List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray) {
    Opener opener = new Opener();
    ImagePlus imagePlus = opener.openImage(filePath);
    ImageProcessor processor = imagePlus.getProcessor();
    List<float[]> result = new ArrayList<>();
    for (List<Integer> startAndEndTime : startAndEndTimeArray) {
      int start = startAndEndTime.get(0);
      int end = startAndEndTime.get(1);
      int length = end - start + 1;
      float[] pixels = new float[length];
      if (processor instanceof FloatProcessor) {
        for (int i = start; i <= end; i++) {
          pixels[i - start] = processor.getf(i);
        }
      }
      result.add(pixels);
    }
    return result;
  }
}
