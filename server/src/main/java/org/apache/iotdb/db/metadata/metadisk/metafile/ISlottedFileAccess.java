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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * this interface provides operations on a certain file which is consisted of a header and a series
 * of slot/record with fixed length
 *
 * <p>the data is read in form of block which consists of several continuous records
 */
public interface ISlottedFileAccess {

  long getFileLength() throws IOException;

  int getHeaderLength();

  int getBlockSize();

  ByteBuffer readHeader() throws IOException;

  void writeHeader(ByteBuffer buffer) throws IOException;

  ByteBuffer readBytes(long position, int length) throws IOException;

  void readBytes(long position, ByteBuffer buffer) throws IOException;

  void writeBytes(long position, ByteBuffer buffer) throws IOException;

  void sync() throws IOException;

  void close() throws IOException;
}
