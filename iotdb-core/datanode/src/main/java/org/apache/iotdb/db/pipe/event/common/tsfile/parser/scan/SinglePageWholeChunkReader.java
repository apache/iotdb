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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.scan;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.file.header.ChunkHeader;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.reader.chunk.AbstractChunkReader;
import org.apache.tsfile.read.reader.page.PageReader;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import static org.apache.tsfile.file.metadata.enums.CompressionType.UNCOMPRESSED;

public class SinglePageWholeChunkReader extends AbstractChunkReader {
  private final ChunkHeader chunkHeader;
  private final ByteBuffer chunkDataBuffer;
  private final EncryptParameter encryptParam;

  public SinglePageWholeChunkReader(Chunk chunk) throws IOException {
    super(Long.MIN_VALUE, null);

    this.chunkHeader = chunk.getHeader();
    this.chunkDataBuffer = chunk.getData();
    this.encryptParam = chunk.getEncryptParam();
    initAllPageReaders();
  }

  private void initAllPageReaders() throws IOException {
    // construct next satisfied page header
    while (chunkDataBuffer.remaining() > 0) {
      pageReaderList.add(
          constructPageReader(
              PageHeader.deserializeFrom(
                  chunkDataBuffer, (Statistics<? extends Serializable>) null)));
    }
  }

  private PageReader constructPageReader(PageHeader pageHeader) throws IOException {
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    return new PageReader(
        pageHeader,
        deserializePageData(pageHeader, chunkDataBuffer, chunkHeader, decryptor),
        chunkHeader.getDataType(),
        Decoder.getDecoderByType(chunkHeader.getEncodingType(), chunkHeader.getDataType()),
        defaultTimeDecoder,
        null);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // util methods
  /////////////////////////////////////////////////////////////////////////////////////////////////

  public static ByteBuffer readCompressedPageData(PageHeader pageHeader, ByteBuffer chunkBuffer)
      throws IOException {
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    byte[] compressedPageBody = new byte[compressedPageBodyLength];
    // doesn't have a complete page body
    if (compressedPageBodyLength > chunkBuffer.remaining()) {
      throw new IOException(
          "do not has a complete page body. Expected:"
              + compressedPageBodyLength
              + ". Actual:"
              + chunkBuffer.remaining());
    }
    chunkBuffer.get(compressedPageBody);
    return ByteBuffer.wrap(compressedPageBody);
  }

  public static ByteBuffer uncompressPageData(
      PageHeader pageHeader, IUnCompressor unCompressor, ByteBuffer compressedPageData)
      throws IOException {
    if (unCompressor.getCodecName() == UNCOMPRESSED) {
      return compressedPageData;
    }
    int compressedPageBodyLength = pageHeader.getCompressedSize();
    ByteBuffer uncompressedPageData = ByteBuffer.allocate(pageHeader.getUncompressedSize());
    try {
      unCompressor.uncompress(
          compressedPageData.array(), 0, compressedPageBodyLength, uncompressedPageData.array(), 0);
    } catch (Exception e) {
      throw new IOException(
          "Uncompress error! uncompress size: "
              + pageHeader.getUncompressedSize()
              + "compressed size: "
              + pageHeader.getCompressedSize()
              + "page header: "
              + pageHeader
              + e.getMessage(),
          e);
    }

    return uncompressedPageData;
  }

  public static ByteBuffer decrypt(IDecryptor decryptor, ByteBuffer buffer) {
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return buffer;
    }
    return ByteBuffer.wrap(
        decryptor.decrypt(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining()));
  }

  public static ByteBuffer decryptAndUncompressPageData(
      PageHeader pageHeader,
      IUnCompressor unCompressor,
      ByteBuffer compressedPageData,
      IDecryptor decryptor)
      throws IOException {
    return uncompressPageData(pageHeader, unCompressor, decrypt(decryptor, compressedPageData));
  }

  public static ByteBuffer deserializePageData(
      PageHeader pageHeader, ByteBuffer chunkBuffer, ChunkHeader chunkHeader, IDecryptor decryptor)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(chunkHeader.getCompressionType());
    ByteBuffer compressedPageBody = readCompressedPageData(pageHeader, chunkBuffer);
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return uncompressPageData(pageHeader, unCompressor, compressedPageBody);
    } else {
      return decryptAndUncompressPageData(pageHeader, unCompressor, compressedPageBody, decryptor);
    }
  }
}
