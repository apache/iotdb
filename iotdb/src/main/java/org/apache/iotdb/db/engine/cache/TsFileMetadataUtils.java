/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.cache;

import org.apache.iotdb.tsfile.file.metadata.TsDeviceMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is used to read metadata(<code>TsFileMetaData</code> and <code>TsRowGroupBlockMetaData</code>).
 * 
 * @author liukun
 *
 */
public class TsFileMetadataUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TsFileMetadataUtils.class);

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
