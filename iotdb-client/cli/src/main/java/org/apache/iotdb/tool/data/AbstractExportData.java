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

package org.apache.iotdb.tool.data;

import org.apache.iotdb.cli.i18n.CliMessages;
import org.apache.iotdb.exception.ArgsErrorException;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.session.Session;

import java.io.IOException;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/** Model-specific exporters share configuration and explicit resource ownership. */
public abstract class AbstractExportData implements AutoCloseable {
  private static final Pattern AGGREGATE =
      Pattern.compile(
          "\\b(count|sum|avg|extreme|max_value|min_value|first_value|last_value|max_time|min_time|stddev|stddev_pop|stddev_samp|variance|var_pop|var_samp|max_by|min_by)\\b\\s*\\(",
          Pattern.CASE_INSENSITIVE);
  protected final ExportConfig config;
  protected final ExportProgressReporter progress;
  protected ZoneId zoneId;
  protected String timestampPrecision;
  private final List<Resource<?>> resources = new ArrayList<>();
  private boolean closed;

  AbstractExportData(ExportConfig config, ExportProgressReporter progress) {
    this.config = config;
    this.progress = progress;
  }

  public abstract void init() throws Exception;

  abstract void export(ExportTask task) throws Exception;

  protected void readTimeSettings(Session session) throws Exception {
    session.open(false);
    timestampPrecision = session.getTimestampPrecision();
    if (config.timeZone != null) {
      session.setTimeZone(config.timeZone);
    }
    zoneId = ZoneId.of(session.getTimeZone());
  }

  ExportTimeRange timeRange() throws ArgsErrorException {
    return ExportTimeRange.parse(config.startTime, config.endTime, zoneId, timestampPrecision);
  }

  protected static void legalCheck(String sql) throws ArgsErrorException {
    if (AGGREGATE.matcher(sql).find()) {
      throw new ArgsErrorException(
          CliMessages.EXCEPTION_AGGREGATE_QUERIES_CANNOT_BE_EXPORTED_AS_SQL_OR_TSFILE_48BC1932);
    }
  }

  /** With -dt, value columns are written as name(TYPE); time and the Tree Device column are not. */
  protected List<String> csvHeaders(List<String> names, List<String> types, boolean tree) {
    List<String> headers = new ArrayList<>(names);
    if (config.printDataType) {
      for (int i = 0; i < headers.size(); i++) {
        String name = headers.get(i);
        if (!"Time".equalsIgnoreCase(name) && !(tree && "Device".equals(name))) {
          headers.set(i, name + "(" + types.get(i) + ")");
        }
      }
    }
    return headers;
  }

  /** One value of an INSERT statement, in a form both SQL dialects read back. */
  protected static String sqlValue(SessionDataSet.DataIterator iterator, String type, int column)
      throws Exception {
    if (iterator.isNull(column)) {
      return "NULL";
    }
    switch (type) {
      case "TEXT":
      case "STRING":
      case "DATE":
        return "'" + iterator.getString(column).replace("'", "''") + "'";
      case "TIMESTAMP":
        return Long.toString(iterator.getLong(column));
      case "BLOB":
        return "X'" + iterator.getString(column).substring(2) + "'";
      default:
        return iterator.getString(column);
    }
  }

  protected String timeTrans(long time) {
    if ("timestamp".equals(config.timeFormat)
        || "number".equals(config.timeFormat)
        || "long".equals(config.timeFormat)) {
      return Long.toString(time);
    }
    DateTimeFormatter formatter =
        "default".equals(config.timeFormat)
            ? DateTimeFormatter.ISO_OFFSET_DATE_TIME
            : DateTimeFormatter.ofPattern(config.timeFormat);
    return RpcUtils.parseLongToDateWithPrecision(formatter, time, zoneId, timestampPrecision);
  }

  protected <T extends AutoCloseable> Resource<T> own(T resource) throws Exception {
    synchronized (resources) {
      if (closed) {
        resource.close();
        throw new IOException(CliMessages.EXCEPTION_EXPORT_CANCELLED_85D44F54);
      }
      Resource<T> owned = new Resource<>(resource);
      resources.add(owned);
      return owned;
    }
  }

  /** Closing from the shutdown hook and normal execution releases each handle only once. */
  protected final class Resource<T extends AutoCloseable> implements AutoCloseable {
    private final T value;
    private boolean released;

    private Resource(T value) {
      this.value = value;
    }

    T get() {
      return value;
    }

    @Override
    public synchronized void close() throws Exception {
      if (!released) {
        released = true;
        try {
          value.close();
        } finally {
          synchronized (resources) {
            resources.remove(this);
          }
        }
      }
    }
  }

  @Override
  public synchronized void close() throws Exception {
    List<Resource<?>> remaining;
    synchronized (resources) {
      if (closed) {
        return;
      }
      closed = true;
      remaining = new ArrayList<>(resources);
    }
    Collections.reverse(remaining);
    Exception failure = null;
    for (Resource<?> resource : remaining) {
      try {
        resource.close();
      } catch (Exception e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
