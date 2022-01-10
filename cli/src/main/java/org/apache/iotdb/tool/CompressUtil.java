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
package org.apache.iotdb.tool;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

public class CompressUtil {

  public static void snappyCompress(InputStream fi, OutputStream fo) {

    byte[] buffer = new byte[1024 * 1024 * 8];
    SnappyFramedOutputStream sout = null;
    try {
      sout = new SnappyFramedOutputStream(fo);
      while (true) {
        int count = fi.read(buffer, 0, buffer.length);
        if (count == -1) {
          break;
        }
        sout.write(buffer, 0, count);
      }
      sout.flush();
    } catch (Throwable ex) {
      ex.printStackTrace();
    } finally {
      if (sout != null) {
        try {
          sout.close();
        } catch (Exception e) {
        }
      }
      if (fi != null) {
        try {
          fi.close();
        } catch (Exception x) {
        }
      }
    }
  }

  public static void snappyUncompress(InputStream fi, OutputStream fo)
      throws UnsupportedEncodingException, IOException {
    byte[] buffer = new byte[1024 * 1024 * 8];
    SnappyFramedInputStream sin = null;
    try {
      sin = new SnappyFramedInputStream(fi);

      while (true) {
        int count = sin.read(buffer, 0, buffer.length);
        if (count == -1) {
          break;
        }
        fo.write(buffer, 0, count);
      }
      fo.flush();
    } catch (Throwable ex) {
      ex.printStackTrace();
    } finally {
      if (sin != null) {
        try {
          sin.close();
        } catch (Exception x) {
        }
      }
      if (fi != null) {
        try {
          fi.close();
        } catch (Exception x) {
        }
      }
    }
  }

  public static void gzipCompress(InputStream fi, OutputStream fo) {

    byte[] buffer = new byte[1024 * 1024 * 8];
    GzipCompressorOutputStream sout = null;
    try {
      sout = new GzipCompressorOutputStream(fo);
      while (true) {
        int count = fi.read(buffer, 0, buffer.length);
        if (count == -1) {
          break;
        }
        sout.write(buffer, 0, count);
      }
      sout.flush();
    } catch (Throwable ex) {
      ex.printStackTrace();
    } finally {
      if (sout != null) {
        try {
          sout.close();
        } catch (Exception e) {
        }
      }
      if (fi != null) {
        try {
          fi.close();
        } catch (Exception x) {
        }
      }
    }
  }

  public static void gzipUncompress(InputStream fi, OutputStream fo)
      throws UnsupportedEncodingException, IOException {
    byte[] buffer = new byte[1024 * 1024 * 8];
    GzipCompressorInputStream sin = null;
    try {
      sin = new GzipCompressorInputStream(fi);

      while (true) {
        int count = sin.read(buffer, 0, buffer.length);
        if (count == -1) {
          break;
        }
        fo.write(buffer, 0, count);
      }
      fo.flush();
    } catch (Throwable ex) {
      ex.printStackTrace();
    } finally {
      if (sin != null) {
        try {
          sin.close();
        } catch (Exception x) {
        }
      }
      if (fi != null) {
        try {
          fi.close();
        } catch (Exception x) {
        }
      }
    }
  }
}
