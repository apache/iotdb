/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.openapi.gen.handler.exception;

import org.apache.iotdb.openapi.gen.handler.ApiResponseMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class BaseExceptionMapper implements ExceptionMapper<Exception> {
  private static Logger log = LoggerFactory.getLogger(BaseExceptionMapper.class);

  @Override
  public Response toResponse(Exception exception) {
    log.error("toResponse() caught exception", exception);
    Response resp =
        Response.status(Status.INTERNAL_SERVER_ERROR)
            .entity(new ApiResponseMessage(ApiResponseMessage.ERROR, exception.getMessage()))
            .build();

    return resp;
  }
}
