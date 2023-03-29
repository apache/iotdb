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

package org.apache.iotdb.db.engine.modification.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Copied from {@link java.io.BufferedReader}, trace the read position by modifying the fill()
 * method.
 */
public class TracedBufferedReader extends Reader {
  private Reader in;

  private char cb[];
  private int nChars, nextChar;

  private static final int INVALIDATED = -2;
  private static final int UNMARKED = -1;
  private int markedChar = UNMARKED;
  private int readAheadLimit = 0; /* Valid only when markedChar > 0 */

  /** If the next character is a line feed, skip it */
  private boolean skipLF = false;

  /** The skipLF flag when the mark was set */
  private boolean markedSkipLF = false;

  private static int defaultCharBufferSize = 8192;
  private static int defaultExpectedLineLength = 80;

  /** the total bytes number already filled into cb */
  private long totalFilledBytesNum = 0;

  /**
   * Creates a buffering character-input stream that uses an input buffer of the specified size.
   *
   * @param in A Reader
   * @param sz Input-buffer size
   * @exception IllegalArgumentException If {@code sz <= 0}
   */
  public TracedBufferedReader(Reader in, int sz) {
    super(in);
    if (sz <= 0) {
      throw new IllegalArgumentException("Buffer size <= 0");
    }
    this.in = in;
    cb = new char[sz];
    nextChar = nChars = 0;
  }

  /**
   * Creates a buffering character-input stream that uses a default-sized input buffer.
   *
   * @param in A Reader
   */
  public TracedBufferedReader(Reader in) {
    this(in, defaultCharBufferSize);
  }

  /** Checks to make sure that the stream has not been closed */
  private void ensureOpen() throws IOException {
    if (in == null) {
      throw new IOException("Stream closed");
    }
  }

  /** {@link BufferedReader#fill()} */
  private void fill() throws IOException {
    int dst;
    if (markedChar <= UNMARKED) {
      /* No mark */
      dst = 0;
    } else {
      /* Marked */
      int delta = nextChar - markedChar;
      if (delta >= readAheadLimit) {
        /* Gone past read-ahead limit: Invalidate mark */
        markedChar = INVALIDATED;
        readAheadLimit = 0;
        dst = 0;
      } else {
        if (readAheadLimit <= cb.length) {
          /* Shuffle in the current buffer */
          System.arraycopy(cb, markedChar, cb, 0, delta);
          markedChar = 0;
          dst = delta;
        } else {
          /* Reallocate buffer to accommodate read-ahead limit */
          char ncb[] = new char[readAheadLimit];
          System.arraycopy(cb, markedChar, ncb, 0, delta);
          cb = ncb;
          markedChar = 0;
          dst = delta;
        }
        nextChar = nChars = delta;
      }
    }

    int n;
    do {
      n = in.read(cb, dst, cb.length - dst);
    } while (n == 0);
    if (n > 0) {
      nChars = dst + n;
      nextChar = dst;
      totalFilledBytesNum = totalFilledBytesNum + n;
    }
  }

  /** {@link BufferedReader#read()} */
  @Override
  public int read() throws IOException {
    synchronized (lock) {
      ensureOpen();
      for (; ; ) {
        if (nextChar >= nChars) {
          fill();
          if (nextChar >= nChars) {
            return -1;
          }
        }
        if (skipLF) {
          skipLF = false;
          if (cb[nextChar] == '\n') {
            nextChar++;
            continue;
          }
        }
        return cb[nextChar++];
      }
    }
  }

  /** {@link BufferedReader#read1(char[], int, int)} */
  private int read1(char[] cbuf, int off, int len) throws IOException {
    if (nextChar >= nChars) {
      /* If the requested length is at least as large as the buffer, and
      if there is no mark/reset activity, and if line feeds are not
      being skipped, do not bother to copy the characters into the
      local buffer.  In this way buffered streams will cascade
      harmlessly. */
      if (len >= cb.length && markedChar <= UNMARKED && !skipLF) {
        return in.read(cbuf, off, len);
      }
      fill();
    }
    if (nextChar >= nChars) {
      return -1;
    }
    if (skipLF) {
      skipLF = false;
      if (cb[nextChar] == '\n') {
        nextChar++;
        if (nextChar >= nChars) {
          fill();
        }
        if (nextChar >= nChars) {
          return -1;
        }
      }
    }
    int n = Math.min(len, nChars - nextChar);
    System.arraycopy(cb, nextChar, cbuf, off, n);
    nextChar += n;
    return n;
  }

  /** {@link BufferedReader#read(char[], int, int)} */
  @Override
  public int read(char cbuf[], int off, int len) throws IOException {
    synchronized (lock) {
      ensureOpen();
      if ((off < 0)
          || (off > cbuf.length)
          || (len < 0)
          || ((off + len) > cbuf.length)
          || ((off + len) < 0)) {
        throw new IndexOutOfBoundsException();
      } else if (len == 0) {
        return 0;
      }

      int n = read1(cbuf, off, len);
      if (n <= 0) {
        return n;
      }
      while ((n < len) && in.ready()) {
        int n1 = read1(cbuf, off + n, len - n);
        if (n1 <= 0) {
          break;
        }
        n += n1;
      }
      return n;
    }
  }

  /** {@link BufferedReader#readLine(boolean)} */
  String readLine(boolean ignoreLF) throws IOException {
    StringBuilder s = null;
    int startChar;

    synchronized (lock) {
      ensureOpen();
      boolean omitLF = ignoreLF || skipLF;

      bufferLoop:
      for (; ; ) {

        if (nextChar >= nChars) {
          fill();
        }
        if (nextChar >= nChars) {
          /* EOF */
          if (s != null && s.length() > 0) {
            return s.toString();
          } else {
            return null;
          }
        }
        boolean eol = false;
        char c = 0;
        int i;

        /* Skip a leftover '\n', if necessary */
        if (omitLF && (cb[nextChar] == '\n')) {
          nextChar++;
        }
        skipLF = false;
        omitLF = false;

        charLoop:
        for (i = nextChar; i < nChars; i++) {
          c = cb[i];
          if ((c == '\n') || (c == '\r')) {
            eol = true;
            break charLoop;
          }
        }

        startChar = nextChar;
        nextChar = i;

        if (eol) {
          String str;
          if (s == null) {
            str = new String(cb, startChar, i - startChar);
          } else {
            s.append(cb, startChar, i - startChar);
            str = s.toString();
          }
          nextChar++;
          if (c == '\r') {
            skipLF = true;
            if (read() != -1) {
              nextChar--;
            }
          }
          return str;
        }

        if (s == null) {
          s = new StringBuilder(defaultExpectedLineLength);
        }
        s.append(cb, startChar, i - startChar);
      }
    }
  }

  /** {@link BufferedReader#readLine()} */
  public String readLine() throws IOException {
    return readLine(false);
  }

  /** {@link BufferedReader#skip(long)} */
  @Override
  public long skip(long n) throws IOException {
    if (n < 0L) {
      throw new IllegalArgumentException("skip value is negative");
    }
    synchronized (lock) {
      ensureOpen();
      long r = n;
      while (r > 0) {
        if (nextChar >= nChars) {
          fill();
        }
        if (nextChar >= nChars) {
          /* EOF */
          break;
        }
        if (skipLF) {
          skipLF = false;
          if (cb[nextChar] == '\n') {
            nextChar++;
          }
        }
        long d = (long) nChars - nextChar;
        if (r <= d) {
          nextChar += r;
          r = 0;
          break;
        } else {
          r -= d;
          nextChar = nChars;
        }
      }
      return n - r;
    }
  }

  /** {@link BufferedReader#ready()} */
  @Override
  public boolean ready() throws IOException {
    synchronized (lock) {
      ensureOpen();

      /*
       * If newline needs to be skipped and the next char to be read
       * is a newline character, then just skip it right away.
       */
      if (skipLF) {
        /* Note that in.ready() will return true if and only if the next
         * read on the stream will not block.
         */
        if (nextChar >= nChars && in.ready()) {
          fill();
        }
        if (nextChar < nChars) {
          if (cb[nextChar] == '\n') {
            nextChar++;
          }
          skipLF = false;
        }
      }
      return (nextChar < nChars) || in.ready();
    }
  }

  /** {@link BufferedReader#markSupported()} */
  @Override
  public boolean markSupported() {
    return true;
  }

  /** {@link BufferedReader#mark(int)} */
  @Override
  public void mark(int readAheadLimit) throws IOException {
    if (readAheadLimit < 0) {
      throw new IllegalArgumentException("Read-ahead limit < 0");
    }
    synchronized (lock) {
      ensureOpen();
      this.readAheadLimit = readAheadLimit;
      markedChar = nextChar;
      markedSkipLF = skipLF;
    }
  }

  /** {@link BufferedReader#reset()} */
  @Override
  public void reset() throws IOException {
    synchronized (lock) {
      ensureOpen();
      if (markedChar < 0) {
        throw new IOException((markedChar == INVALIDATED) ? "Mark invalid" : "Stream not marked");
      }
      nextChar = markedChar;
      skipLF = markedSkipLF;
    }
  }

  /** {@link BufferedReader#close()} */
  @Override
  public void close() throws IOException {
    synchronized (lock) {
      if (in == null) {
        return;
      }
      try {
        in.close();
      } finally {
        in = null;
        cb = null;
      }
    }
  }

  /** {@link BufferedReader#lines()} */
  public Stream<String> lines() {
    Iterator<String> iter =
        new Iterator<String>() {
          String nextLine = null;

          @Override
          public boolean hasNext() {
            if (nextLine != null) {
              return true;
            } else {
              try {
                nextLine = readLine();
                return (nextLine != null);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          }

          @Override
          public String next() {
            if (nextLine != null || hasNext()) {
              String line = nextLine;
              nextLine = null;
              return line;
            } else {
              throw new NoSuchElementException();
            }
          }
        };
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED | Spliterator.NONNULL),
        false);
  }

  /**
   * Returns this reader's file position.
   *
   * @return This reader's file position, a non-negative integer counting the number of bytes from
   *     the beginning of the file to the current position
   */
  public long position() {
    // position = totalFilledBytesNum - lastFilledBytesNum + readOffsetInLastFilledBytes
    // lastFilledBytesNum = nChars - dst, readOffsetInLastFilledBytes = nextChar - dst
    return totalFilledBytesNum - nChars + nextChar;
  }
}
