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

package org.apache.iotdb.commons.pipe.sink.compressor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_GZIP;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_LZ4;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_LZMA2;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_SNAPPY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_DEFAULT_VALUE;

public class PipeCompressorFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeCompressorFactory.class);

  private static final Map<String, PipeCompressor> COMPRESSOR_NAME_TO_INSTANCE =
      new ConcurrentHashMap<>();

  static {
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_SNAPPY, new PipeSnappyCompressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_GZIP, new PipeGZIPCompressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_LZ4, new PipeLZ4Compressor());
    COMPRESSOR_NAME_TO_INSTANCE.put(
        CONNECTOR_COMPRESSOR_ZSTD,
        new PipeZSTDCompressor(CONNECTOR_COMPRESSOR_ZSTD_LEVEL_DEFAULT_VALUE));
    COMPRESSOR_NAME_TO_INSTANCE.put(CONNECTOR_COMPRESSOR_LZMA2, new PipeLZMA2Compressor());
  }

  public static PipeCompressor getCompressor(PipeCompressorConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("PipeCompressorConfig is null");
    }
    if (config.getName() == null) {
      throw new IllegalArgumentException("PipeCompressorConfig.getName() is null");
    }

    final String compressorName = config.getName();

    // For ZSTD compressor, we need to consider the compression level
    if (compressorName.equals(CONNECTOR_COMPRESSOR_ZSTD)) {
      final int zstdCompressionLevel = config.getZstdCompressionLevel();
      return COMPRESSOR_NAME_TO_INSTANCE.computeIfAbsent(
          CONNECTOR_COMPRESSOR_ZSTD + "_" + zstdCompressionLevel,
          key -> {
            LOGGER.info("Create new PipeZSTDCompressor with level: {}", zstdCompressionLevel);
            return new PipeZSTDCompressor(zstdCompressionLevel);
          });
    }

    // For other compressors, we can directly get the instance by name
    final PipeCompressor compressor = COMPRESSOR_NAME_TO_INSTANCE.get(compressorName);
    if (compressor != null) {
      return compressor;
    }

    throw new UnsupportedOperationException("PipeCompressor not found for name: " + compressorName);
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
