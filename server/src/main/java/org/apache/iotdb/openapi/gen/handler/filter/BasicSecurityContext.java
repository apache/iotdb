package org.apache.iotdb.openapi.gen.handler.filter;

import org.apache.iotdb.openapi.gen.handler.model.User;

import javax.ws.rs.core.SecurityContext;

import java.security.Principal;

public class BasicSecurityContext implements SecurityContext {

  private final User user;
  private final boolean secure;

  public BasicSecurityContext(User user, boolean secure) {
    this.user = user;
    this.secure = secure;
  }

  @Override
  public Principal getUserPrincipal() {
    return new Principal() {
      @Override
      public String getName() {
        return user.getUsername();
      }
    };
  }

  public User getUser() {
    return user;
  }

  @Override
  public boolean isUserInRole(String s) {
    return false;
  }

  @Override
  public boolean isSecure() {
    return false;
  }

  @Override
  public String getAuthenticationScheme() {
    return null;
  }
}
