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

package org.apache.iotdb.postgresql.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes the server's messages of protocol 3.0 into a buffer. Each method appends one complete
 * message; streaming rows use {@link #beginDataRow}, {@link #value}, {@link #nullValue} and {@link
 * #end}.
 */
public final class BackendMessages {

  /** One column of a RowDescription. */
  public static final class Field {
    final String name;
    final int tableOid;
    final short columnNumber;
    final int typeOid;
    final short typeSize;
    final int typeModifier;
    final short format;

    public Field(String name, int typeOid, short typeSize, short format) {
      this(name, 0, (short) 0, typeOid, typeSize, -1, format);
    }

    public Field(
        String name,
        int tableOid,
        short columnNumber,
        int typeOid,
        short typeSize,
        int typeModifier,
        short format) {
      this.name = name;
      this.tableOid = tableOid;
      this.columnNumber = columnNumber;
      this.typeOid = typeOid;
      this.typeSize = typeSize;
      this.typeModifier = typeModifier;
      this.format = format;
    }

    public short format() {
      return format;
    }
  }

  private BackendMessages() {}

  /** The single byte answering an SSLRequest or GSSENCRequest (not a framed message). */
  public static void sslResponse(ByteBuf out, boolean accept) {
    out.writeByte(accept ? 'S' : 'N');
  }

  public static void authenticationOk(ByteBuf out) {
    int start = begin(out, 'R');
    out.writeInt(0);
    end(out, start);
  }

  public static void authenticationCleartextPassword(ByteBuf out) {
    int start = begin(out, 'R');
    out.writeInt(3);
    end(out, start);
  }

  public static void parameterStatus(ByteBuf out, String name, String value) {
    int start = begin(out, 'S');
    cstring(out, name);
    cstring(out, value);
    end(out, start);
  }

  public static void backendKeyData(ByteBuf out, int processId, int secretKey) {
    int start = begin(out, 'K');
    out.writeInt(processId);
    out.writeInt(secretKey);
    end(out, start);
  }

  /** {@code status}: 'I' idle, 'T' in a transaction block, 'E' in a failed transaction block. */
  public static void readyForQuery(ByteBuf out, char status) {
    int start = begin(out, 'Z');
    out.writeByte(status);
    end(out, start);
  }

  public static void negotiateProtocolVersion(
      ByteBuf out, int newestMinorVersion, List<String> unsupportedOptions) {
    int start = begin(out, 'v');
    out.writeInt(newestMinorVersion);
    out.writeInt(unsupportedOptions.size());
    for (String option : unsupportedOptions) {
      cstring(out, option);
    }
    end(out, start);
  }

  public static void rowDescription(ByteBuf out, List<Field> fields) {
    int start = begin(out, 'T');
    out.writeShort(fields.size());
    for (Field field : fields) {
      cstring(out, field.name);
      out.writeInt(field.tableOid);
      out.writeShort(field.columnNumber);
      out.writeInt(field.typeOid);
      out.writeShort(field.typeSize);
      out.writeInt(field.typeModifier);
      out.writeShort(field.format);
    }
    end(out, start);
  }

  /** Starts a DataRow; returns the position to pass to {@link #end}. */
  public static int beginDataRow(ByteBuf out, int columns) {
    int start = begin(out, 'D');
    out.writeShort(columns);
    return start;
  }

  public static void value(ByteBuf out, byte[] value) {
    out.writeInt(value.length);
    out.writeBytes(value);
  }

  public static void nullValue(ByteBuf out) {
    out.writeInt(-1);
  }

  public static void commandComplete(ByteBuf out, String tag) {
    int start = begin(out, 'C');
    cstring(out, tag);
    end(out, start);
  }

  public static void emptyQueryResponse(ByteBuf out) {
    end(out, begin(out, 'I'));
  }

  public static void parseComplete(ByteBuf out) {
    end(out, begin(out, '1'));
  }

  public static void bindComplete(ByteBuf out) {
    end(out, begin(out, '2'));
  }

  public static void closeComplete(ByteBuf out) {
    end(out, begin(out, '3'));
  }

  public static void noData(ByteBuf out) {
    end(out, begin(out, 'n'));
  }

  public static void portalSuspended(ByteBuf out) {
    end(out, begin(out, 's'));
  }

  public static void parameterDescription(ByteBuf out, int[] typeOids) {
    int start = begin(out, 't');
    out.writeShort(typeOids.length);
    for (int oid : typeOids) {
      out.writeInt(oid);
    }
    end(out, start);
  }

  /** An ErrorResponse with severity ERROR or FATAL. */
  public static void errorResponse(ByteBuf out, PgException error) {
    response(
        out,
        'E',
        error.isFatal() ? "FATAL" : "ERROR",
        error.sqlState(),
        error.getMessage(),
        error.hint());
  }

  public static void noticeResponse(ByteBuf out, String severity, String sqlState, String message) {
    response(out, 'N', severity, sqlState, message, null);
  }

  private static void response(
      ByteBuf out, char type, String severity, String sqlState, String message, String hint) {
    int start = begin(out, type);
    field(out, 'S', severity);
    field(out, 'V', severity);
    field(out, 'C', sqlState);
    field(out, 'M', message == null ? "" : message);
    if (hint != null) {
      field(out, 'H', hint);
    }
    out.writeByte(0);
    end(out, start);
  }

  private static void field(ByteBuf out, char code, String value) {
    out.writeByte(code);
    cstring(out, value);
  }

  /** Writes the type byte and a placeholder length; returns the length's position. */
  public static int begin(ByteBuf out, char type) {
    out.writeByte(type);
    int lengthIndex = out.writerIndex();
    out.writeInt(0);
    return lengthIndex;
  }

  /** Fills in the length of the message started at {@code lengthIndex}. */
  public static void end(ByteBuf out, int lengthIndex) {
    out.setInt(lengthIndex, out.writerIndex() - lengthIndex);
  }

  public static void cstring(ByteBuf out, String value) {
    out.writeBytes(value.getBytes(StandardCharsets.UTF_8));
    out.writeByte(0);
  }
}
