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

import ij.ImagePlus;
import ij.io.FileSaver;
import ij.process.FloatProcessor;

public class UnCompressedTiffModelWriter extends ModelWriter {
  @Override
  void write(String filePath, float[] values, int width, int height) {
    FloatProcessor floatProcessor = new FloatProcessor(width, height, values);
    ImagePlus imp = new ImagePlus("first level", floatProcessor);
    FileSaver fs = new FileSaver(imp);
    fs.saveAsTiff(filePath);
  }
}
