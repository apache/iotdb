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

package org.apache.iotdb.collector.api.v1.impl;

import org.apache.iotdb.collector.agent.CollectorAgent;
import org.apache.iotdb.collector.api.v1.AdminApiService;
import org.apache.iotdb.collector.api.v1.NotFoundException;
import org.apache.iotdb.collector.api.v1.handler.RequestValidationHandler;
import org.apache.iotdb.collector.api.v1.model.AlterPipeRequest;
import org.apache.iotdb.collector.api.v1.model.CreatePipeRequest;
import org.apache.iotdb.collector.api.v1.model.DropPipeRequest;
import org.apache.iotdb.collector.api.v1.model.StartPipeRequest;
import org.apache.iotdb.collector.api.v1.model.StopPipeRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

public class AdminApiServiceImpl extends AdminApiService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AdminApiServiceImpl.class);

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
    RequestValidationHandler.validateCreateRequest(createPipeRequest);

    final boolean createdResult =
        CollectorAgent.task()
            .createCollectorTask(
                createPipeRequest.getSourceAttribute(),
                createPipeRequest.getProcessorAttribute(),
                createPipeRequest.getSinkAttribute(),
                createPipeRequest.getTaskId());
    if (createdResult) {
      LOGGER.info("Create task successful");
      return Response.status(Response.Status.OK).entity("create task success").build();
    }
    LOGGER.warn("Create task failed");
    return Response.status(Response.Status.BAD_REQUEST).entity("create task fail").build();
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
    RequestValidationHandler.validateStopRequest(stopPipeRequest);

    final boolean stopResult = CollectorAgent.task().stopCollectorTask(stopPipeRequest.getTaskId());
    if (stopResult) {
      LOGGER.info("Stop task: {} successful", stopPipeRequest.getTaskId());
      return Response.ok().entity("stop task: " + stopPipeRequest.getTaskId() + " success").build();
    }
    LOGGER.warn("Stop task: {} failed", stopPipeRequest.getTaskId());
    return Response.status(Response.Status.BAD_REQUEST).entity("stop task fail").build();
  }
}
