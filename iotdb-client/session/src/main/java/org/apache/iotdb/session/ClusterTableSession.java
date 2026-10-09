/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.session;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.encoding.table.ClusterTable;
import org.apache.tsfile.encoding.table.ClusterTableCodec;
import org.apache.tsfile.encoding.table.ClusterTableEncoder;
import org.apache.tsfile.encoding.table.ClusterTableOptions;
import org.apache.tsfile.encoding.table.ClusterTableTsFile;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.RowRecord;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Complete-record clustering over ordinary IoTDB BLOB storage. One dedicated device is one numeric
 * table stream. The outer timestamp is a unique BLOCK identifier, never an original row timestamp.
 * Use this reader to decode original fields; ordinary SQL sees only block metadata.
 *
 * <p>The caller owns the session and supplies collision-free increasing block IDs, including across
 * restarts. Do not apply original-row time filters or TTL to the outer block timestamp. This writer
 * is single-threaded. A failed insert may have committed remotely: retry the same block/table
 * through a new writer after checking its outcome, rather than choosing a new ID.
 */
public final class ClusterTableSession {
  private static final List<String> MEASUREMENTS =
      Collections.unmodifiableList(Arrays.asList("payload", "min_time", "max_time", "row_count"));
  private static final List<TSDataType> TYPES =
      Collections.unmodifiableList(
          Arrays.asList(TSDataType.BLOB, TSDataType.INT64, TSDataType.INT64, TSDataType.INT32));
  private final ISession session;
  private final String device;
  private final ClusterTableEncoder encoder;
  private boolean written;
  private boolean failed;
  private long lastBlockId;

  public ClusterTableSession(ISession session, String device, ClusterTableOptions options) {
    if (session == null
        || device == null
        || !device.matches("root(?:\\.[A-Za-z_][A-Za-z0-9_]*)+")) {
      throw new IllegalArgumentException("Expected a dedicated simple root.device path");
    }
    this.session = session;
    this.device = device;
    this.encoder = new ClusterTableEncoder(options);
  }

  public void createSchema() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < MEASUREMENTS.size(); i++) {
      String path = device + "." + MEASUREMENTS.get(i);
      if (!session.checkTimeseriesExists(path)) {
        session.createTimeseries(path, TYPES.get(i), TSEncoding.PLAIN, CompressionType.LZ4);
      }
    }
  }

  /** Writes one independently decodable page; returns its actual serialized byte count. */
  public int writeBlock(long blockId, ClusterTable page)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    if (failed)
      throw new IOException("Prior insert outcome is uncertain; reconcile before resuming");
    if (written && blockId <= lastBlockId) {
      throw new IllegalArgumentException("Block IDs must increase within this writer");
    }
    if (page.rowCount() == 0) throw new IllegalArgumentException("Cannot store an empty page");
    ClusterTableCodec.Encoded encoded = encoder.encode(page);
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (int r = 0; r < page.rowCount(); r++) {
      min = Math.min(min, page.timestamp(r));
      max = Math.max(max, page.timestamp(r));
    }
    byte[] bytes = encoded.bytes();
    try {
      session.insertRecord(
          device,
          blockId,
          MEASUREMENTS,
          TYPES,
          Arrays.asList(new Binary(bytes), min, max, page.rowCount()));
    } catch (IoTDBConnectionException | StatementExecutionException error) {
      failed = true;
      throw error;
    }
    written = true;
    lastBlockId = blockId;
    return bytes.length;
  }

  /** Fits on leading rows, splits into bounded pages, and returns the number of blocks written. */
  public int writeTable(long firstBlockId, ClusterTable table)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    encoder.fit(table);
    int pageSize = ClusterTableEncoder.pageSize(table);
    int blocks = (table.rowCount() + pageSize - 1) / pageSize;
    if (blocks > 0) Math.addExact(firstBlockId, blocks - 1L);
    for (int i = 0; i < blocks; i++) {
      int from = i * pageSize;
      writeBlock(firstBlockId + i, table.slice(from, Math.min(table.rowCount(), from + pageSize)));
    }
    return blocks;
  }

  public void readAll(ClusterTableTsFile.BlockConsumer consumer)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    read("select payload from " + device, Long.MIN_VALUE, Long.MAX_VALUE, consumer);
  }

  /** Inclusive original-time filter: block bounds prune candidates, then exact row filtering. */
  public void readTimeRange(long start, long end, ClusterTableTsFile.BlockConsumer consumer)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    if (start > end) throw new IllegalArgumentException("Reversed time range");
    String sql =
        "select payload from " + device + " where min_time <= " + end + " and max_time >= " + start;
    read(sql, start, end, consumer);
  }

  private void read(String sql, long start, long end, ClusterTableTsFile.BlockConsumer consumer)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try (SessionDataSet result = session.executeQueryStatement(sql)) {
      while (result.hasNext()) {
        RowRecord record = result.next();
        if (record.getFields().isEmpty() || record.getFields().get(0).getDataType() == null) {
          throw new IOException("Missing cluster block payload");
        }
        ClusterTable table =
            ClusterTableCodec.decode(record.getFields().get(0).getBinaryV().getValues());
        if (start != Long.MIN_VALUE || end != Long.MAX_VALUE) table = table.timeRange(start, end);
        if (table.rowCount() != 0) consumer.accept(table);
      }
    }
  }
}
