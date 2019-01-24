/**
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
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to read metadata(<code>TsFileMetaData</code> and
 * <code>TsRowGroupBlockMetaData</code>).
 *
 * @author liukun
 */
public class TsFileMetadataUtils {

  /**
   * get tsfile meta data.
   *
   * @param filePath -given path
   * @return -meta data
   */
  public static TsFileMetaData getTsFileMetaData(String filePath) throws IOException {
    TsFileSequenceReader reader = null;
    try {
      reader = new TsFileSequenceReader(filePath);
      return reader.readFileMetadata();
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * get row group block meta data.
   *
   * @param filePath -file path
   * @param deviceId -device id
   * @param fileMetaData -ts file meta data
   * @return -device meta data
   */
  public static TsDeviceMetadata getTsRowGroupBlockMetaData(String filePath, String deviceId,
      TsFileMetaData fileMetaData) throws IOException {
    if (!fileMetaData.getDeviceMap().containsKey(deviceId)) {
      return null;
    } else {
      TsFileSequenceReader reader = null;
      try {
        reader = new TsFileSequenceReader(filePath);
        long offset = fileMetaData.getDeviceMap().get(deviceId).getOffset();
        int size = fileMetaData.getDeviceMap().get(deviceId).getLen();
        ByteBuffer data = ByteBuffer.allocate(size);
        reader.readRaw(offset, size, data);
        data.flip();
        return TsDeviceMetadata.deserializeFrom(data);
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }
  }
}
