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

import java.util.List;

public abstract class ModelReader {
  /**
   * Read part of float values from a file, eg. tsfile, tiff
   *
   * @param filePath the path of the file to read
   * @param startAndEndTimeArray a list of start and end time pairs, each pair is a list of two
   *     integers
   * @return a list of float arrays, each array corresponds to a start and end time pair
   */
  abstract List<float[]> penetrate(String filePath, List<List<Integer>> startAndEndTimeArray);

  public ModelReader getInstance(ModelReaderType modelFileType) {
    switch (modelFileType) {
      case UNCOMPRESSED_TIFF:
        return new UnCompressedTiffModelReader();
      default:
        return new CompressedTsFileModelReader();
    }
  }
}
