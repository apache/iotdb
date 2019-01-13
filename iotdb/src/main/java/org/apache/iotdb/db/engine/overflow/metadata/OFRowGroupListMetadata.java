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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Metadata of overflow RowGroup list
 */
public class OFRowGroupListMetadata {

    private String deviceId;
    private List<OFSeriesListMetadata> seriesList;

    private OFRowGroupListMetadata() {
    }

    public OFRowGroupListMetadata(String deviceId) {
        this.deviceId = deviceId;
        seriesList = new ArrayList<>();
    }

    /**
     * add OFSeriesListMetadata metadata to list
     * 
     * @param timeSeries
     */
    public void addSeriesListMetaData(OFSeriesListMetadata timeSeries) {
        if (seriesList == null) {
            seriesList = new ArrayList<OFSeriesListMetadata>();
        }
        seriesList.add(timeSeries);
    }

    public List<OFSeriesListMetadata> getSeriesList() {
        return seriesList == null ? null : Collections.unmodifiableList(seriesList);
    }

    @Override
    public String toString() {
        return String.format("OFRowGroupListMetadata{ deviceId id: %s, series Lists: %s }", deviceId,
                seriesList.toString());
    }

    public String getdeviceId() {
        return deviceId;
    }

    public int serializeTo(OutputStream outputStream) throws IOException {
        int byteLen = 0;
        byteLen += ReadWriteIOUtils.write(deviceId, outputStream);
        int size = seriesList.size();
        byteLen += ReadWriteIOUtils.write(size, outputStream);
        for (OFSeriesListMetadata ofSeriesListMetadata : seriesList) {
            byteLen += ofSeriesListMetadata.serializeTo(outputStream);
        }
        return byteLen;
    }

    public int serializeTo(ByteBuffer buffer) throws IOException {
        throw new NotImplementedException();
    }

    public static OFRowGroupListMetadata deserializeFrom(InputStream inputStream) throws IOException {
        OFRowGroupListMetadata ofRowGroupListMetadata = new OFRowGroupListMetadata();
        ofRowGroupListMetadata.deviceId = ReadWriteIOUtils.readString(inputStream);
        int size = ReadWriteIOUtils.readInt(inputStream);
        List<OFSeriesListMetadata> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(OFSeriesListMetadata.deserializeFrom(inputStream));
        }
        ofRowGroupListMetadata.seriesList = list;
        return ofRowGroupListMetadata;
    }

    public static OFRowGroupListMetadata deserializeFrom(ByteBuffer buffer) throws IOException {
        throw new NotImplementedException();
    }
}
