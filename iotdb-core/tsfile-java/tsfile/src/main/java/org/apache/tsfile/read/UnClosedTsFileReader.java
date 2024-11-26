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

package org.apache.tsfile.read;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.exception.NotImplementedException;
import org.apache.tsfile.file.metadata.TsFileMetadata;

import java.io.IOException;
import java.util.function.LongConsumer;

/** A class for reading unclosed tsfile. */
public class UnClosedTsFileReader extends TsFileSequenceReader {

  private EncryptParameter encryptParam;

  // ioSizeRecorder can be null
  public UnClosedTsFileReader(String file, LongConsumer ioSizeRecorder) throws IOException {
    super(file, false, ioSizeRecorder);
    encryptParam = EncryptUtils.encryptParam;
  }

  // ioSizeRecorder can be null
  public UnClosedTsFileReader(
      String file, EncryptParameter decryptParam, LongConsumer ioSizeRecorder) throws IOException {
    super(file, false, ioSizeRecorder);
    this.encryptParam = encryptParam;
  }

  /** unclosed file has no tail magic data. */
  @Override
  public String readTailMagic() {
    throw new NotImplementedException();
  }

  /** unclosed file has no file metadata. */
  @Override
  public TsFileMetadata readFileMetadata() {
    throw new NotImplementedException();
  }

  @Override
  public EncryptParameter getEncryptParam() {
    return encryptParam;
  }
}
