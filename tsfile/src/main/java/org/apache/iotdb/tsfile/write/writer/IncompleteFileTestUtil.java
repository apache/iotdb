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
package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Util to write Incomplete files which can be used for tests.
 */
public class IncompleteFileTestUtil {

    /**
     * Should not be initialized.
     */
    private IncompleteFileTestUtil() {
        throw new UnsupportedOperationException();
    }

    /**
     * Writes a File with one incomplete chunk header
     * @param file File to write
     * @throws IOException is thrown when encountering IO issues
     */
    public static void writeFileWithOneIncompleteChunkHeader(File file) throws IOException {
        TsFileWriter writer = new TsFileWriter(file);

        ChunkHeader header = new ChunkHeader("s1", 100, TSDataType.FLOAT, CompressionType.SNAPPY,
                TSEncoding.PLAIN, 5);
        ByteBuffer buffer = ByteBuffer.allocate(header.getSerializedSize());
        header.serializeTo(buffer);
        buffer.flip();
        byte[] data = new byte[3];
        buffer.get(data, 0, 3);
        writer.getIOWriter().out.write(data);
        writer.getIOWriter().forceClose();
    }
}
