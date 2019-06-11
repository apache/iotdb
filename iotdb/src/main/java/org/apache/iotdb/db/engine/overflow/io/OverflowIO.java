/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.overflow.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

import org.apache.iotdb.tsfile.read.reader.TsFileInput;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

public class OverflowIO extends TsFileIOWriter {

  private OverflowReadWriter overflowReadWriter;

  public OverflowIO(OverflowReadWriter overflowReadWriter) throws IOException {
    super(overflowReadWriter, new ArrayList<>());
    this.overflowReadWriter = overflowReadWriter;
    toTail();
  }

  public void clearRowGroupMetadatas() {
    super.flushedChunkGroupMetaDataList.clear();
  }

  @Override
  public long getPos() throws IOException {
    return overflowReadWriter.getPosition();
  }

  public void toTail() throws IOException {
    overflowReadWriter.toTail();
  }

  public void close() throws IOException {
    overflowReadWriter.close();
  }

  public void flush() throws IOException {
    overflowReadWriter.flush();
  }

  public TsFileInput getReader() {
    return overflowReadWriter;
  }

  public TsFileOutput getWriter() {
    return overflowReadWriter;
  }

  public OutputStream getOutputStream() {
    return overflowReadWriter;
  }

  public static class OverflowReadWriter extends OutputStream implements TsFileOutput, TsFileInput {

    private static final String RW_MODE = "rw";
    private RandomAccessFile raf;

    public OverflowReadWriter(String filepath) throws FileNotFoundException {
      this.raf = new RandomAccessFile(filepath, RW_MODE);
    }

    public void toTail() throws IOException {
      raf.seek(raf.length());
    }

    @Override
    public long size() throws IOException {
      return raf.length();
    }

    @Override
    public long position() throws IOException {
      return raf.getFilePointer();
    }

    @Override
    public TsFileInput position(long newPosition) throws IOException {
      raf.seek(newPosition);
      return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      return raf.getChannel().read(dst);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
      return raf.getChannel().read(dst, position);
    }

    @Override
    public FileChannel wrapAsFileChannel() throws IOException {
      return raf.getChannel();
    }

    @Override
    public InputStream wrapAsInputStream() throws IOException {
      return Channels.newInputStream(raf.getChannel());
    }

    @Override
    public int read() throws IOException {
      return raf.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      raf.readFully(b, off, len);
      return len;
    }

    @Override
    public int readInt() throws IOException {
      return raf.readInt();
    }

    @Override
    public void write(ByteBuffer b) throws IOException {
      raf.getChannel().write(b);
    }

    @Override
    public long getPosition() throws IOException {
      return raf.getFilePointer();
    }

    @Override
    public OutputStream wrapAsStream() throws IOException {
      return Channels.newOutputStream(raf.getChannel());
    }

    @Override
    public void truncate(long position) throws IOException {
      raf.getChannel().truncate(position);
    }

    @Override
    public void write(int b) throws IOException {
      raf.write(b);
    }

    @Override
    public void close() throws IOException {
      raf.close();
    }
  }
}
