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

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_GZIP;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_LZ4;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_LZMA2;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_SNAPPY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_COMPRESSOR_ZSTD;

public class PipeCompressorFactory {

  private static Map<String, PipeCompressor> COMPRESSOR_NAME_TO_INSTANCE = new HashMap<>();

  static {
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_SNAPPY, new PipeSnappyCompressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_GZIP, new PipeGZIPCompressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_LZ4, new PipeLZ4Compressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(
        CONNECTOR_COMPRESSOR_ZSTD,
        new PipeZSTDCompressor(
            PipeConfig.getInstance().getPipeConnectorRPCCompressionZSTDCompressorLevel()));
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_LZMA2, new PipeLZMA2Compressor());
    COMPRESSOR_NAME_TO_INSTANCE = Collections.unmodifiableMap(COMPRESSOR_NAME_TO_INSTANCE);
  }

  public static PipeCompressor getCompressor(String name) {
    final PipeCompressor compressor = COMPRESSOR_NAME_TO_INSTANCE.get(name);
    if (compressor == null) {
      throw new UnsupportedOperationException("PipeCompressor not found for name: " + name);
    }
    return compressor;
  }

  private static Map<Byte, PipeCompressor> COMPRESSOR_INDEX_TO_INSTANCE = new HashMap<>();

  static {
    COMPRESSOR_INDEX_TO_INSTANCE.put(
        PipeCompressor.PipeCompressionType.SNAPPY.getIndex(),
        COMPRESSOR_NAME_TO_INSTANCE.get(CONNECTOR_COMPRESSOR_SNAPPY));
    COMPRESSOR_INDEX_TO_INSTANCE.put(
        PipeCompressor.PipeCompressionType.GZIP.getIndex(),
        COMPRESSOR_NAME_TO_INSTANCE.get(CONNECTOR_COMPRESSOR_GZIP));
    COMPRESSOR_INDEX_TO_INSTANCE.put(
        PipeCompressor.PipeCompressionType.LZ4.getIndex(),
        COMPRESSOR_NAME_TO_INSTANCE.get(CONNECTOR_COMPRESSOR_LZ4));
    COMPRESSOR_INDEX_TO_INSTANCE.put(
        PipeCompressor.PipeCompressionType.ZSTD.getIndex(),
        COMPRESSOR_NAME_TO_INSTANCE.get(CONNECTOR_COMPRESSOR_ZSTD));
    COMPRESSOR_INDEX_TO_INSTANCE.put(
        PipeCompressor.PipeCompressionType.LZMA2.getIndex(),
        COMPRESSOR_NAME_TO_INSTANCE.get(CONNECTOR_COMPRESSOR_LZMA2));
    COMPRESSOR_INDEX_TO_INSTANCE = Collections.unmodifiableMap(COMPRESSOR_INDEX_TO_INSTANCE);
  }

  public static PipeCompressor getCompressor(byte index) {
    final PipeCompressor compressor = COMPRESSOR_INDEX_TO_INSTANCE.get(index);
    if (compressor == null) {
      throw new UnsupportedOperationException("PipeCompressor not found for index: " + index);
    }
    return compressor;
  }

  private PipeCompressorFactory() {
    // Empty constructor
  }
}
