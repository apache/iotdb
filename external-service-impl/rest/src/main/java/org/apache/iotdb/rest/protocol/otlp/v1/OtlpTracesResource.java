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

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

/**
 * OTLP/HTTP traces endpoint. OpenTelemetry fixes the signal suffix to {@code /v1/traces}; IoTDB
 * serves the base under {@code /rest/otlp} so the full URL is {@code /rest/otlp/v1/traces} and
 * clients configure {@code OTEL_EXPORTER_OTLP_ENDPOINT=http://<host>:18080/rest/otlp}.
 */
@Path("/rest/v1/otlp/v1/traces")
public class OtlpTracesResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(OtlpTracesResource.class);

  @POST
  public Response export(@Context final HttpHeaders headers, final byte[] body) {
    final boolean protobuf = OtlpHttp.isProtobuf(headers);
    try {
      final ExportTraceServiceRequest request =
          OtlpHttp.parse(body, ExportTraceServiceRequest.newBuilder(), protobuf).build();
      final boolean ok = OtlpService.getInstance().ingestTraces(request);
      if (!ok) {
        return OtlpHttp.partialFailure(protobuf, "OTLP trace insert failed");
      }
      return OtlpHttp.success(ExportTraceServiceResponse.getDefaultInstance(), protobuf);
    } catch (final Exception e) {
      LOGGER.warn("OTLP trace export failed", e);
      return OtlpHttp.badRequest(protobuf, e.getMessage());
    }
  }
}
