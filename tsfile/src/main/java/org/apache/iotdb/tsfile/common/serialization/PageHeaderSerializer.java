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
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class PageHeaderSerializer implements IDataSerializer<PageHeader, TSDataType> {

    @Override
    public int serializeTo(PageHeader data, OutputStream outputStream) throws IOException {
        int length = ReadWriteIOUtils.write(data.getUncompressedSize(), outputStream);
        length+=ReadWriteIOUtils.write(data.getCompressedSize(), outputStream);
        length+=data.getStatistics().serialize(outputStream);
        return length;
    }
    @Override
    public int serializeTo(PageHeader data, ByteBuffer buffer) {
        int length = ReadWriteIOUtils.write(data.getUncompressedSize(), buffer);
        length+=ReadWriteIOUtils.write(data.getCompressedSize(), buffer);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            length+=data.getStatistics().serialize(stream);
        } catch (IOException ex) {
            // i know but in this case never happens an exception.
            return 0;
        }
        buffer.put(stream.toByteArray());
        return length;
    }

    @Override
    public PageHeader deserializeFrom(ByteBuffer buffer, TSDataType options) {
        int uncompressedSize = ReadWriteIOUtils.readInt(buffer);
        int compressedSize = ReadWriteIOUtils.readInt(buffer);
        Statistics statistics = Statistics.deserialize(buffer, options);
        return new PageHeader(uncompressedSize, compressedSize, statistics);
    }
    @Override
    public PageHeader deserializeFrom(InputStream inputStream, TSDataType options) throws IOException {
        int uncompressedSize = ReadWriteIOUtils.readInt(inputStream);
        int compressedSize = ReadWriteIOUtils.readInt(inputStream);
        Statistics statistics = Statistics.deserialize(inputStream, options);
        return new PageHeader(uncompressedSize, compressedSize, statistics);
    }
}
