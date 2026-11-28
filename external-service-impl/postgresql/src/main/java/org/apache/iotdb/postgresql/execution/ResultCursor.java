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

package org.apache.iotdb.postgresql.execution;

import org.apache.iotdb.postgresql.i18n.PostgreSqlMessages;
import org.apache.iotdb.postgresql.protocol.BackendMessages;
import org.apache.iotdb.postgresql.protocol.PgException;
import org.apache.iotdb.postgresql.protocol.SqlState;
import org.apache.iotdb.postgresql.types.PgType;
import org.apache.iotdb.postgresql.types.PgValueEncoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Streams the rows of a {@link RunningQuery} as DataRow messages: block by block, writing only
 * while the channel is writable, so memory stays bounded however large the result is. Keeps its
 * position, so a portal can continue after PortalSuspended.
 */
public final class ResultCursor implements AutoCloseable {

  /**
   * A client that stopped reading for longer than the write timeout, or that disconnected while
   * rows were still being sent ({@link #clientGone()}).
   */
  public static final class WriteTimeoutException extends Exception {
    private static final long serialVersionUID = 1L;

    private final boolean clientGone;

    WriteTimeoutException() {
      this(false);
    }

    WriteTimeoutException(boolean clientGone) {
      super(clientGone ? "client disconnected" : "client stopped reading", null, false, false);
      this.clientGone = clientGone;
    }

    /** True if the connection is closed: nothing can be sent any more. */
    public boolean clientGone() {
      return clientGone;
    }
  }

  /** Bytes of DataRows collected before they are handed to the channel. */
  private static final int BATCH_BYTES = 64 * 1024;

  private final RunningQuery query;
  private final PgType[] types;
  private short[] formats;
  private final PgValueEncoder encoder;
  private TsBlock block;
  private int position;
  private boolean exhausted;

  /**
   * @param formats result formats as sent in Bind: none (all text), one (for all columns) or one
   *     per column; 0 text, 1 binary
   */
  public ResultCursor(RunningQuery query, short[] formats, PgValueEncoder encoder)
      throws PgException {
    this.query = query;
    this.encoder = encoder;
    List<TSDataType> dataTypes = query.types();
    this.types = new PgType[dataTypes.size()];
    for (int i = 0; i < types.length; i++) {
      TSDataType dataType = dataTypes.get(i);
      Optional<PgType> type = PgType.of(dataType);
      if (!type.isPresent()) {
        throw new PgException(
            SqlState.FEATURE_NOT_SUPPORTED,
            "column \""
                + query.names().get(i)
                + "\" has type "
                + dataType
                + ", which is not"
                + " available through PostgreSQL");
      }
      types[i] = type.get();
    }
    this.formats = expand(formats, types.length);
  }

  private static short[] expand(short[] formats, int columns) throws PgException {
    short[] result = new short[columns];
    if (formats == null || formats.length == 0) {
      return result;
    }
    if (formats.length == 1) {
      java.util.Arrays.fill(result, formats[0]);
      return result;
    }
    if (formats.length != columns) {
      throw new PgException(
          SqlState.PROTOCOL_VIOLATION,
          "bind message has "
              + formats.length
              + " result formats but query has "
              + columns
              + " columns");
    }
    return formats.clone();
  }

  /**
   * Changes the result formats for the rows sent from now on: a SQL cursor's FETCH chooses them per
   * fetch (Bind of the FETCH, or BINARY of the DECLARE).
   */
  public void setFormats(short[] formats) throws PgException {
    this.formats = expand(formats, types.length);
  }

  /** The RowDescription fields. */
  public List<BackendMessages.Field> fields() {
    List<BackendMessages.Field> fields = new ArrayList<>(types.length);
    for (int i = 0; i < types.length; i++) {
      fields.add(
          new BackendMessages.Field(
              query.names().get(i), types[i].oid(), types[i].size(), formats[i]));
    }
    return fields;
  }

  /** True once every row was sent. */
  public boolean exhausted() {
    return exhausted;
  }

  /**
   * Sends up to {@code maxRows} rows (0 = all) as DataRow messages, appending to {@code out} and
   * handing full batches to the channel.
   *
   * @return the number of rows sent
   */
  public long send(Channel channel, ByteBuf out, long maxRows, Duration writeTimeout)
      throws PgException, WriteTimeoutException {
    return advance(channel, out, maxRows, writeTimeout);
  }

  /**
   * Skips up to {@code maxRows} rows (0 = all) without sending them (SQL {@code MOVE}).
   *
   * @return the number of rows skipped
   */
  public long skip(long maxRows) throws PgException {
    try {
      return advance(null, null, maxRows, null);
    } catch (WriteTimeoutException e) {
      throw new IllegalStateException(
          PostgreSqlMessages.EXCEPTION_NOTHING_IS_WRITTEN_WHILE_SKIPPING_5DE21CBD, e);
    }
  }

  /** Sends ({@code out != null}) or skips rows. */
  private long advance(Channel channel, ByteBuf out, long maxRows, Duration writeTimeout)
      throws PgException, WriteTimeoutException {
    long sent = 0;
    while (maxRows == 0 || sent < maxRows) {
      if (channel != null && !channel.isActive()) {
        // the client disconnected: stop reading the result instead of encoding it for nobody
        throw new WriteTimeoutException(true);
      }
      if (block == null || position >= block.getPositionCount()) {
        Optional<TsBlock> next = query.next();
        if (!next.isPresent()) {
          exhausted = true;
          break;
        }
        block = next.get();
        position = 0;
      }
      int end = block.getPositionCount();
      if (maxRows > 0) {
        end = (int) Math.min(end, position + (maxRows - sent));
      }
      if (out == null) {
        sent += end - position;
        position = end;
        continue;
      }
      Column[] columns = new Column[types.length];
      for (int c = 0; c < columns.length; c++) {
        columns[c] = block.getColumn(query.blockIndex(c));
      }
      for (; position < end; position++) {
        int row = BackendMessages.beginDataRow(out, columns.length);
        for (int c = 0; c < columns.length; c++) {
          if (columns[c].isNull(position)) {
            BackendMessages.nullValue(out);
          } else if (formats[c] == 0) {
            BackendMessages.value(out, encoder.text(types[c], columns[c], position));
          } else {
            BackendMessages.value(out, encoder.binary(types[c], columns[c], position));
          }
        }
        BackendMessages.end(out, row);
        sent++;
        if (out.readableBytes() >= BATCH_BYTES) {
          flush(channel, out, writeTimeout);
        }
      }
    }
    return sent;
  }

  /** Hands the collected bytes to the channel and waits until it can take more. */
  public static void flush(Channel channel, ByteBuf out, Duration writeTimeout)
      throws WriteTimeoutException {
    if (out.readableBytes() == 0) {
      return;
    }
    if (!channel.isActive()) {
      out.clear();
      throw new WriteTimeoutException(true);
    }
    // a copy: the caller keeps using its buffer while Netty writes this one
    channel.writeAndFlush(out.copy());
    out.clear();
    awaitWritable(channel, writeTimeout);
  }

  private static void awaitWritable(Channel channel, Duration writeTimeout)
      throws WriteTimeoutException {
    long deadline = System.nanoTime() + writeTimeout.toNanos();
    long pause = 1;
    while (!channel.isWritable() && channel.isActive()) {
      if (System.nanoTime() > deadline) {
        throw new WriteTimeoutException(false);
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      pause = Math.min(pause * 2, 50);
    }
    if (!channel.isActive()) {
      throw new WriteTimeoutException(true);
    }
  }

  /** Cancels the query (CancelRequest, write timeout). */
  public void cancel() {
    query.cancel();
  }

  @Override
  public void close() {
    query.close();
  }
}
