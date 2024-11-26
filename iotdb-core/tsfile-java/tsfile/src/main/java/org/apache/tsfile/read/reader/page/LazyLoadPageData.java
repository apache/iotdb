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

package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.file.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazyLoadPageData {
  /** Reference to the data of original chunkDataBuffer. * */
  private final byte[] chunkData;

  private final int pageDataOffset;

  private final IUnCompressor unCompressor;

  private final EncryptParameter encryptParam;

  public LazyLoadPageData(byte[] data, int offset, IUnCompressor unCompressor) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.encryptParam = EncryptUtils.encryptParam;
  }

  public LazyLoadPageData(
      byte[] data, int offset, IUnCompressor unCompressor, EncryptParameter encryptParam) {
    this.chunkData = data;
    this.pageDataOffset = offset;
    this.unCompressor = unCompressor;
    this.encryptParam = encryptParam;
  }

  public ByteBuffer uncompressPageData(PageHeader pageHeader) throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] uncompressedPageData = new byte[pageHeader.getUncompressedSize()];
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    byte[] decryptedPageData =
        decryptor.decrypt(chunkData, pageDataOffset, compressedPageBodyLength);
    try {
      unCompressor.uncompress(
          decryptedPageData, 0, compressedPageBodyLength, uncompressedPageData, 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage());
    }
    return ByteBuffer.wrap(uncompressedPageData);
  }

  public IUnCompressor getUnCompressor() {
    return unCompressor;
  }
}
