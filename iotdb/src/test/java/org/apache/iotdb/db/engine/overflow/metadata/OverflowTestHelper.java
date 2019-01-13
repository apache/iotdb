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
package org.apache.iotdb.db.engine.overflow.metadata;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsDigest;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class OverflowTestHelper {

    private static String deviceId = "device";
    public static final String MEASUREMENT_UID = "sensor231";
    public static final long FILE_OFFSET = 2313424242L;
    public static final long NUM_OF_POINTS = 123456L;
    public static final long START_TIME = 523372036854775806L;
    public static final long END_TIME = 523372036854775806L;
    public static final TSDataType DATA_TYPE = TSDataType.INT64;

    public static ChunkMetaData createSimpleTimeSeriesChunkMetaData() {
        ChunkMetaData metaData = new ChunkMetaData(MEASUREMENT_UID, DATA_TYPE, FILE_OFFSET, START_TIME, END_TIME// ,
                                                                                                                // ChunkMetaDataTest.ENCODING_TYPE
        );
        metaData.setNumOfPoints(NUM_OF_POINTS);
        metaData.setDigest(new TsDigest());
        return metaData;
    }

    public static List<ChunkMetaData> createChunkMetaDataList(int count) {
        List<ChunkMetaData> ret = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ret.add(createSimpleTimeSeriesChunkMetaData());
        }
        return ret;
    }

    public static OFSeriesListMetadata createOFSeriesListMetadata() {
        OFSeriesListMetadata ofSeriesListMetadata = new OFSeriesListMetadata(MEASUREMENT_UID,
                createChunkMetaDataList(5));
        return ofSeriesListMetadata;
    }

    public static OFRowGroupListMetadata createOFRowGroupListMetadata() {
        OFRowGroupListMetadata ofRowGroupListMetadata = new OFRowGroupListMetadata(deviceId);
        for (int i = 0; i < 5; i++) {
            ofRowGroupListMetadata.addSeriesListMetaData(createOFSeriesListMetadata());
        }
        return ofRowGroupListMetadata;
    }

    public static OFFileMetadata createOFFileMetadata() {
        OFFileMetadata ofFileMetadata = new OFFileMetadata();
        ofFileMetadata.setLastFooterOffset(100);
        for (int i = 0; i < 5; i++) {
            ofFileMetadata.addRowGroupListMetaData(createOFRowGroupListMetadata());
        }
        return ofFileMetadata;
    }

    public static List<String> getJSONArray() {
        List<String> jsonMetaData = new ArrayList<String>();
        jsonMetaData.add("fsdfsfsd");
        jsonMetaData.add("424fd");
        return jsonMetaData;
    }
}
