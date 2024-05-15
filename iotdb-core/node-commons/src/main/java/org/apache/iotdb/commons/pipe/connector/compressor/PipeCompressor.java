/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.pipe.connector.compressor;

import java.io.IOException;
import java.util.Objects;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_LZMA2;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_SNAPPY;

public abstract class PipeCompressor {

  public enum PipeCompressionType {
    SNAPPY((byte) 0),
    LZMA2((byte) 1);

    final byte index;

    PipeCompressionType(byte index) {
      this.index = index;
    }
  }

  private final PipeCompressionType compressionType;

  protected PipeCompressor(PipeCompressionType compressionType) {
    this.compressionType = compressionType;
  }

  public abstract byte[] compress(byte[] data) throws IOException;

  public abstract byte[] decompress(byte[] byteArray) throws IOException;

  public static PipeCompressor getCompressor(String name) {
    if (Objects.equals(name, CONNECTOR_COMPRESSOR_SNAPPY)) {
      return new PipeSnappyCompressor();
    } else if (Objects.equals(name, CONNECTOR_COMPRESSOR_LZMA2)) {
      return new PipeLZMA2Compressor();
    } else {
      throw new UnsupportedOperationException("PipeCompressor not found: " + name);
    }
  }

  public static PipeCompressor getCompressor(byte index) {
    switch (index) {
      case 0:
        return new PipeSnappyCompressor();
      case 1:
        return new PipeLZMA2Compressor();
      default:
        throw new IllegalArgumentException("Unknown PipeCompressionType index: " + index);
    }
  }

  public byte serialize() {
    return compressionType.index;
  }
}
