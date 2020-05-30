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
package org.apache.iotdb.tsfile.common.serialization;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ChunkHeaderSerializer implements IDataSerializer<ChunkHeader, Boolean> {

    @Override
    public int serializeTo(ChunkHeader data, OutputStream outputStream) throws IOException {
        int length = 0;
        length += ReadWriteIOUtils.write(MetaMarker.CHUNK_HEADER, outputStream);
        length += ReadWriteIOUtils.write(data.getMeasurementID(), outputStream);
        length += ReadWriteIOUtils.write(data.getDataSize(), outputStream);
        length += ReadWriteIOUtils.write(data.getDataType(), outputStream);
        length += ReadWriteIOUtils.write(data.getNumOfPages(), outputStream);
        length += ReadWriteIOUtils.write(data.getCompressionType(), outputStream);
        length += ReadWriteIOUtils.write(data.getEncodingType(), outputStream);
        return length;
    }

    @Override
    public int serializeTo(ChunkHeader data, ByteBuffer buffer) {
        int length = 0;
        length += ReadWriteIOUtils.write(MetaMarker.CHUNK_HEADER, buffer);
        length += ReadWriteIOUtils.write(data.getMeasurementID(), buffer);
        length += ReadWriteIOUtils.write(data.getDataSize(), buffer);
        length += ReadWriteIOUtils.write(data.getDataType(), buffer);
        length += ReadWriteIOUtils.write(data.getNumOfPages(), buffer);
        length += ReadWriteIOUtils.write(data.getCompressionType(), buffer);
        length += ReadWriteIOUtils.write(data.getEncodingType(), buffer);
        return length;
    }

    @Override
    public ChunkHeader deserializeFrom(ByteBuffer buffer, Boolean options) {
        int size = buffer.getInt();
        String measurementID = ReadWriteIOUtils.readStringWithLength(buffer, size);
        int dataSize = ReadWriteIOUtils.readInt(buffer);
        TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readShort(buffer));
        int numOfPages = ReadWriteIOUtils.readInt(buffer);
        CompressionType type = ReadWriteIOUtils.readCompressionType(buffer);
        TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
        return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages);
    }

    @Override
    public ChunkHeader deserializeFrom(InputStream inputStream, Boolean options) throws IOException{
        if (!options) {
            byte marker = (byte) inputStream.read();
            if (marker != MetaMarker.CHUNK_HEADER) {
                MetaMarker.handleUnexpectedMarker(marker);
            }
        }

        String measurementID = ReadWriteIOUtils.readString(inputStream);
        int dataSize = ReadWriteIOUtils.readInt(inputStream);
        TSDataType dataType = TSDataType.deserialize(ReadWriteIOUtils.readShort(inputStream));
        int numOfPages = ReadWriteIOUtils.readInt(inputStream);
        CompressionType type = ReadWriteIOUtils.readCompressionType(inputStream);
        TSEncoding encoding = ReadWriteIOUtils.readEncoding(inputStream);
        return new ChunkHeader(measurementID, dataSize, dataType, type, encoding, numOfPages);
    }
}
