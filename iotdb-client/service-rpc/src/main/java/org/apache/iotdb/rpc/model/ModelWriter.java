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

public abstract class ModelWriter {
  /**
   * Write float[] to specific type of file, eg. tsfile, tiff
   *
   * @param values the float values of image
   * @param width the width of image
   * @param height the height of image
   * @return the byte array of the file
   */
  abstract byte[] write(float[] values, int width, int height);

  public ModelWriter getInstance(ModelWriterType modelFileType) {
    switch (modelFileType) {
      case UNCOMPRESSED_TIFF:
        return new UnCompressedTiffModelWriter();
      default:
        return new CompressedTsFileModelWriter();
    }
  }
}
