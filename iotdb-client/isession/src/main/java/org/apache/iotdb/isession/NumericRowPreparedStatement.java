/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.
 */
package org.apache.iotdb.isession;

import org.apache.iotdb.isession.i18n.ISessionMessages;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;

import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Immutable metadata and reusable serialization buffers for repeated numeric table row inserts.
 *
 * <p>This statement is not thread-safe. An execution must return before the statement is reused or
 * closed, because its buffers are retained until the synchronous RPC completes.
 */
public final class NumericRowPreparedStatement implements AutoCloseable {
  private final String tableName;
  private final String database;
  private final List<String> columnNames;
  private final List<TSDataType> dataTypes;
  private final List<ColumnCategory> columnCategories;
  private final int tagCount;
  private final int valueBytes;
  private ByteBuffer tagBuffer;
  private ByteBuffer valueBuffer;
  private ByteBuffer nullBitmapBuffer;
  private Object owner;
  private final Map<Object, ConnectionBinding> connectionBindings = new IdentityHashMap<>();
  private boolean closed;

  public NumericRowPreparedStatement(
      String tableName,
      List<String> columnNames,
      List<TSDataType> dataTypes,
      List<ColumnCategory> columnCategories,
      String database) {
    this.tableName = tableName;
    this.database = database;
    this.columnNames = List.copyOf(columnNames);
    this.dataTypes = List.copyOf(dataTypes);
    this.columnCategories = List.copyOf(columnCategories);
    // Column categories are immutable, so repeated inserts can reuse this count.
    this.tagCount =
        (int) this.columnCategories.stream().filter(c -> c == ColumnCategory.TAG).count();
    // FIELD encodings have fixed widths for the lifetime of the prepared schema.
    int bytes = 0;
    for (int i = 0; i < this.dataTypes.size(); i++) {
      if (this.columnCategories.get(i) == ColumnCategory.FIELD) {
        bytes +=
            switch (this.dataTypes.get(i)) {
              case BOOLEAN -> 1;
              case INT32, DATE, FLOAT -> Integer.BYTES;
              case INT64, TIMESTAMP, DOUBLE -> Long.BYTES;
              default ->
                  throw new IllegalArgumentException(
                      ISessionMessages.EXCEPTION_UNSUPPORTED_NUMERIC_TYPE_06DD3044);
            };
      }
    }
    this.valueBytes = bytes;
  }

  public String getTableName() {
    return tableName;
  }

  public String getDatabase() {
    return database;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<ColumnCategory> getColumnCategories() {
    return columnCategories;
  }

  public int getTagCount() {
    return tagCount;
  }

  /**
   * Returns the TAG buffer cleared for writing, growing it when the encoded tags need more space.
   */
  public ByteBuffer prepareTagBuffer(int requiredCapacity) {
    if (tagBuffer == null || tagBuffer.capacity() < requiredCapacity) {
      tagBuffer = ByteBuffer.allocate(requiredCapacity);
    }
    tagBuffer.clear();
    return tagBuffer;
  }

  /** Returns the FIELD buffer cleared for writing; a prepared schema has a fixed encoded size. */
  public ByteBuffer prepareValueBuffer() {
    if (valueBuffer == null) {
      valueBuffer = ByteBuffer.allocate(valueBytes);
    }
    valueBuffer.clear();
    return valueBuffer;
  }

  /** Returns a reusable FIELD null bitmap buffer, allocated only for nullable executions. */
  public ByteBuffer prepareNullBitmapBuffer() {
    if (nullBitmapBuffer == null) {
      nullBitmapBuffer = ByteBuffer.allocate((dataTypes.size() - tagCount + 7) / 8);
    }
    nullBitmapBuffer.clear();
    return nullBitmapBuffer;
  }

  public Object getOwner() {
    return owner;
  }

  public void bind(long id, Object owner, long sessionId, Runnable closeAction) {
    this.owner = owner;
    bindConnection(owner, sessionId, id, closeAction);
    this.closed = false;
  }

  /** Handles belong to a physical session, including its generation after a reconnect. */
  public Long getPreparedId(Object connection, long sessionId) {
    ConnectionBinding binding = connectionBindings.get(connection);
    return binding != null && binding.sessionId == sessionId ? binding.preparedId : null;
  }

  public void bindConnection(Object connection, long sessionId, long id, Runnable closeAction) {
    connectionBindings.put(connection, new ConnectionBinding(sessionId, id, closeAction));
  }

  private record ConnectionBinding(long sessionId, long preparedId, Runnable closeAction) {}

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    try {
      for (ConnectionBinding binding : connectionBindings.values()) {
        binding.closeAction.run();
      }
    } finally {
      connectionBindings.clear();
    }
  }
}
