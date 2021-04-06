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
