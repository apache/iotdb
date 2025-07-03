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

package org.apache.iotdb.commons.pipe.connector.reader;

import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.reader.TsFileInput;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeRemoteFileReader implements PipeSeekableReader {
  private final TsFileInput tsFileInput;

  public PipeRemoteFileReader(File File) throws IOException {
    this.tsFileInput = FSFactoryProducer.getFileInputFactory().getTsFileInput(File.getPath());
  }

  @Override
  public int read(byte[] b) throws IOException {
    ByteBuffer buf = ByteBuffer.wrap(b);
    return tsFileInput.read(buf);
  }

  @Override
  public void seek(long position) throws IOException {
    tsFileInput.position(position);
  }

  @Override
  public void close() throws IOException {
    tsFileInput.close();
  }
}
