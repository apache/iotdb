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

package org.apache.iotdb.collector.protocol.rest.v1.impl;

import org.apache.iotdb.collector.protocol.rest.v1.AdminApiService;
import org.apache.iotdb.collector.protocol.rest.v1.NotFoundException;
import org.apache.iotdb.collector.protocol.rest.v1.model.AlterPipeRequest;
import org.apache.iotdb.collector.protocol.rest.v1.model.CreatePipeRequest;
import org.apache.iotdb.collector.protocol.rest.v1.model.DropPipeRequest;
import org.apache.iotdb.collector.protocol.rest.v1.model.StartPipeRequest;
import org.apache.iotdb.collector.protocol.rest.v1.model.StopPipeRequest;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class AdminApiServiceImpl extends AdminApiService {
  @Override
  public Response alterPipe(
      final AlterPipeRequest alterPipeRequest, final SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok("alterPipe").build();
  }

  @Override
  public Response createPipe(
      final CreatePipeRequest createPipeRequest, final SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok("createPipe").build();
  }

  @Override
  public Response dropPipe(
      final DropPipeRequest dropPipeRequest, final SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok("dropPipe").build();
  }

  @Override
  public Response startPipe(
      final StartPipeRequest startPipeRequest, final SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok("startPipe").build();
  }

  @Override
  public Response stopPipe(
      final StopPipeRequest stopPipeRequest, final SecurityContext securityContext)
      throws NotFoundException {
    return Response.ok("stopPipe").build();
  }
}
