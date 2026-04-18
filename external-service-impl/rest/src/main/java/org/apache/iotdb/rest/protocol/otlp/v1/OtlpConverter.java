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

package org.apache.iotdb.rest.protocol.otlp.v1;

import org.apache.iotdb.commons.conf.CommonDescriptor;

import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import java.util.List;

/** Stateless helpers for converting OTLP protobuf structures into IoTDB row values. */
final class OtlpConverter {

  private static final String TIMESTAMP_PRECISION =
      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();

  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();

  private OtlpConverter() {}

  /**
   * Converts an OTLP Unix-nanoseconds timestamp to the unit IoTDB is currently configured to store.
   * OTLP always emits unsigned 64-bit nanoseconds; we return the value in whatever precision the
   * database uses so it can be handed directly to {@code InsertTabletStatement}.
   */
  static long nanoToDbPrecision(final long unixNano) {
    switch (TIMESTAMP_PRECISION) {
      case "ns":
        return unixNano;
      case "us":
        return unixNano / 1_000L;
      case "ms":
      default:
        return unixNano / 1_000_000L;
    }
  }

  /** Lower-case hex encoding. Empty input yields "" so we never store null IDs. */
  static String bytesToHex(final ByteString bytes) {
    if (bytes == null || bytes.isEmpty()) {
      return "";
    }
    final byte[] data = bytes.toByteArray();
    final char[] out = new char[data.length * 2];
    for (int i = 0; i < data.length; i++) {
      final int b = data[i] & 0xFF;
      out[i * 2] = HEX_CHARS[b >>> 4];
      out[i * 2 + 1] = HEX_CHARS[b & 0x0F];
    }
    return new String(out);
  }

  /** Serializes an OTLP KeyValueList as a JSON object. Returns {@code "{}"} for empty input. */
  static String attributesToJson(final List<KeyValue> attributes) {
    if (attributes == null || attributes.isEmpty()) {
      return "{}";
    }
    final StringBuilder sb = new StringBuilder(64);
    sb.append('{');
    boolean first = true;
    for (final KeyValue kv : attributes) {
      if (!first) {
        sb.append(',');
      }
      first = false;
      appendJsonString(sb, kv.getKey());
      sb.append(':');
      appendAnyValue(sb, kv.getValue());
    }
    sb.append('}');
    return sb.toString();
  }

  /**
   * Fallback database name used when an OTLP request carries no {@code service.name} resource
   * attribute. Matches the OpenTelemetry convention for unnamed services.
   */
  static final String UNKNOWN_SERVICE_DATABASE = "unknown_service";

  /**
   * Derives a valid IoTDB database identifier from an OTLP {@code service.name}. Lower-cases the
   * string and rewrites any character that is not a letter, digit, or underscore into an
   * underscore; prefixes an underscore when the first character would be a digit (IoTDB identifiers
   * must start with a letter or underscore). Empty / null service names fall back to {@link
   * #UNKNOWN_SERVICE_DATABASE}.
   *
   * <p>Examples: {@code "claude-code" -> "claude_code"}, {@code "Gemini CLI" -> "gemini_cli"},
   * {@code "codex" -> "codex"}, {@code "" -> "unknown_service"}.
   */
  static String deriveDatabaseName(final String serviceName) {
    if (serviceName == null || serviceName.isEmpty()) {
      return UNKNOWN_SERVICE_DATABASE;
    }
    final StringBuilder sb = new StringBuilder(serviceName.length());
    for (int i = 0; i < serviceName.length(); i++) {
      final char c = serviceName.charAt(i);
      if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_') {
        sb.append(c);
      } else if (c >= 'A' && c <= 'Z') {
        sb.append((char) (c + 32));
      } else {
        sb.append('_');
      }
    }
    if (sb.length() == 0) {
      return UNKNOWN_SERVICE_DATABASE;
    }
    if (sb.charAt(0) >= '0' && sb.charAt(0) <= '9') {
      sb.insert(0, '_');
    }
    return sb.toString();
  }

  /** Looks up {@code service.name} from a resource attribute list. Returns "" if absent. */
  static String extractServiceName(final List<KeyValue> resourceAttrs) {
    if (resourceAttrs == null) {
      return "";
    }
    for (final KeyValue kv : resourceAttrs) {
      if ("service.name".equals(kv.getKey())) {
        final AnyValue v = kv.getValue();
        if (v != null && v.hasStringValue()) {
          return v.getStringValue();
        }
      }
    }
    return "";
  }

  /** Looks up an attribute by key from a flat attribute list, returning "" if missing. */
  static String extractAttribute(final List<KeyValue> attrs, final String key) {
    if (attrs == null) {
      return "";
    }
    for (final KeyValue kv : attrs) {
      if (key.equals(kv.getKey())) {
        return anyValueToString(kv.getValue());
      }
    }
    return "";
  }

  private static void appendAnyValue(final StringBuilder sb, final AnyValue value) {
    if (value == null) {
      sb.append("null");
      return;
    }
    switch (value.getValueCase()) {
      case STRING_VALUE:
        appendJsonString(sb, value.getStringValue());
        break;
      case BOOL_VALUE:
        sb.append(value.getBoolValue());
        break;
      case INT_VALUE:
        sb.append(value.getIntValue());
        break;
      case DOUBLE_VALUE:
        {
          final double d = value.getDoubleValue();
          if (Double.isFinite(d)) {
            sb.append(d);
          } else {
            appendJsonString(sb, Double.toString(d));
          }
          break;
        }
      case ARRAY_VALUE:
        {
          sb.append('[');
          boolean first = true;
          for (final AnyValue item : value.getArrayValue().getValuesList()) {
            if (!first) {
              sb.append(',');
            }
            first = false;
            appendAnyValue(sb, item);
          }
          sb.append(']');
          break;
        }
      case KVLIST_VALUE:
        {
          sb.append('{');
          boolean first = true;
          for (final KeyValue kv : value.getKvlistValue().getValuesList()) {
            if (!first) {
              sb.append(',');
            }
            first = false;
            appendJsonString(sb, kv.getKey());
            sb.append(':');
            appendAnyValue(sb, kv.getValue());
          }
          sb.append('}');
          break;
        }
      case BYTES_VALUE:
        appendJsonString(sb, bytesToHex(value.getBytesValue()));
        break;
      case VALUE_NOT_SET:
      default:
        sb.append("null");
        break;
    }
  }

  private static String anyValueToString(final AnyValue value) {
    if (value == null) {
      return "";
    }
    switch (value.getValueCase()) {
      case STRING_VALUE:
        return value.getStringValue();
      case BOOL_VALUE:
        return Boolean.toString(value.getBoolValue());
      case INT_VALUE:
        return Long.toString(value.getIntValue());
      case DOUBLE_VALUE:
        return Double.toString(value.getDoubleValue());
      case BYTES_VALUE:
        return bytesToHex(value.getBytesValue());
      default:
        // Fall back to full JSON encoding for nested structures so callers still get a usable
        // string instead of the empty placeholder.
        final StringBuilder sb = new StringBuilder();
        appendAnyValue(sb, value);
        return sb.toString();
    }
  }

  private static void appendJsonString(final StringBuilder sb, final String s) {
    sb.append('"');
    for (int i = 0; i < s.length(); i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        default:
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    sb.append('"');
  }
}
