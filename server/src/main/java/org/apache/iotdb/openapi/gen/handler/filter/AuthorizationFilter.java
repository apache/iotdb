package org.apache.iotdb.openapi.gen.handler.filter;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.authorizer.BasicAuthorizer;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.openapi.gen.handler.ApiResponseMessage;
import org.apache.iotdb.openapi.gen.handler.model.User;

import org.glassfish.jersey.internal.util.Base64;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import java.io.IOException;

@Provider
public class AuthorizationFilter implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext containerRequestContext) throws IOException {
    IAuthorizer authorizer;
    String authzHeader = containerRequestContext.getHeaderString(HttpHeaders.AUTHORIZATION); // (1)
    String decoded = Base64.decodeAsString(authzHeader.replace("Basic ", ""));
    String[] split = decoded.split(":");
    User user = new User();
    user.setUsername(split[0]);
    if (split.length >= 2) {
      user.setPassword(split[1]);
    }
    BasicSecurityContext basicSecurityContext =
        new BasicSecurityContext(user, containerRequestContext.getSecurityContext().isSecure());
    containerRequestContext.setSecurityContext(basicSecurityContext);
    try {
      authorizer = BasicAuthorizer.getInstance();
      boolean b = authorizer.login(split[0], split[1]);
      if (!b) {
        Response resp =
            Response.status(Response.Status.FORBIDDEN)
                .type(MediaType.APPLICATION_JSON)
                .entity(
                    new ApiResponseMessage(
                        ApiResponseMessage.ERROR, "username or passowrd is incorrect!"))
                .build();
        containerRequestContext.abortWith(resp);
      }
    } catch (AuthException e) {
      e.printStackTrace();
    }
  }
}
