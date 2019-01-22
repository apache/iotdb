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
package org.apache.iotdb.db.engine.overflow.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.iotdb.db.engine.overflow.metadata.OFFileMetadata;

/**
 * ConverterUtils is a utility class. It provide conversion between tsfile and thrift overflow metadata class
 */
public class OverflowReadWriteThriftFormatUtils {

  /**
   * read overflow file metadata(thrift format) from stream
   *
   * @param from
   * @throws IOException
   */
  public static OFFileMetadata readOFFileMetaData(InputStream from) throws IOException {
    return OFFileMetadata.deserializeFrom(from);
  }

  /**
   * write overflow metadata(thrift format) to stream
   *
   * @param ofFileMetadata
   * @param to
   * @throws IOException
   */
  public static void writeOFFileMetaData(OFFileMetadata ofFileMetadata, OutputStream to)
      throws IOException {
    ofFileMetadata.serializeTo(to);
  }

}
