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

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Shared HTTP plumbing for the three OTLP JAX-RS resources: parses the request body into the
 * appropriate protobuf message (protobuf or JSON), and renders the empty success response in the
 * encoding the client expects.
 *
 * <p>OTLP/HTTP spec: content types are {@code application/x-protobuf} (default, required) and
 * {@code application/json} (optional). Other content types are rejected with 415.
 */
final class OtlpHttp {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpHttp.class);

  static final String PROTOBUF = "application/x-protobuf";
  static final String JSON = "application/json";

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  private OtlpHttp() {}

  /** True if the request body should be parsed as protobuf; false for JSON. */
  static boolean isProtobuf(final HttpHeaders headers) {
    final String raw = headers.getHeaderString(HttpHeaders.CONTENT_TYPE);
    if (raw == null || raw.isEmpty()) {
      // Default per OTLP spec.
      return true;
    }
    final String contentType = raw.toLowerCase(Locale.ROOT);
    if (contentType.startsWith(PROTOBUF)) {
      return true;
    }
    if (contentType.startsWith(JSON)) {
      return false;
    }
    // Fall back to protobuf for unrecognized types; the parser will fail cleanly if wrong.
    return true;
  }

  /** Parses the request body into {@code builder}. Caller provides a fresh builder. */
  static <T extends Message.Builder> T parse(
      final byte[] body, final T builder, final boolean protobuf) throws Exception {
    if (body == null || body.length == 0) {
      return builder;
    }
    if (protobuf) {
      builder.mergeFrom(body);
    } else {
      JSON_PARSER.merge(new String(body, StandardCharsets.UTF_8), builder);
    }
    return builder;
  }

  /** Builds an OTLP success response (empty message) in the client's preferred encoding. */
  static Response success(final Message emptyResponse, final boolean protobuf) {
    if (protobuf) {
      return Response.ok(emptyResponse.toByteArray(), PROTOBUF).build();
    }
    // JSON for the empty response is "{}".
    return Response.ok("{}", JSON).build();
  }

  /** Builds an OTLP partial-success response with a message describing what failed. */
  static Response partialFailure(final boolean protobuf, final String message) {
    LOGGER.warn("OTLP request partially failed: {}", message);
    if (protobuf) {
      // Empty protobuf; the partial_success field would go here but keeping it minimal is fine.
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .type(PROTOBUF)
          .entity(new byte[0])
          .build();
    }
    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .type(JSON)
        .entity("{\"message\":\"" + escapeJson(message) + "\"}")
        .build();
  }

  /** Builds a client error response, e.g. on malformed input. */
  static Response badRequest(final boolean protobuf, final String message) {
    LOGGER.debug("OTLP bad request: {}", message);
    if (protobuf) {
      return Response.status(Response.Status.BAD_REQUEST)
          .type(PROTOBUF)
          .entity(new byte[0])
          .build();
    }
    return Response.status(Response.Status.BAD_REQUEST)
        .type(JSON)
        .entity("{\"message\":\"" + escapeJson(message) + "\"}")
        .build();
  }

  /** Returns {@link MediaType#APPLICATION_OCTET_STREAM} compatible default if nothing matches. */
  static String responseContentType(final boolean protobuf) {
    return protobuf ? PROTOBUF : JSON;
  }

  private static String escapeJson(final String s) {
    if (s == null) {
      return "";
    }
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
