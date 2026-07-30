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

package org.apache.iotdb.confignode.consensus.request.write.auth;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Objects;

public abstract class AuthorPlan extends ConfigPhysicalReadPlan {

  protected String roleName;
  protected String password;
  protected String newPassword;
  protected String userName;
  protected boolean grantOpt;
  protected int maxSessionPerUser;
  protected int minSessionPerUser;
  protected String newUsername = "";

  // Used for read plans or some write plans whose type name ends with 'V2'
  protected long executedByUserId;

  public AuthorPlan(final ConfigPhysicalPlanType type) {
    super(type);
  }

  // To maintain compatibility between TreeAuthorPlan and RelationalAuthorPlan,
  // the newPassword field is retained here.
  public AuthorPlan(
      ConfigPhysicalPlanType type,
      String userName,
      String roleName,
      String password,
      String newPassword,
      boolean grantOpt,
      int MaxSessionPerUser,
      int MinSessionPerUser) {
    super(type);
    this.userName = userName;
    this.roleName = roleName;
    this.password = password;
    this.newPassword = newPassword;
    this.grantOpt = grantOpt;
    this.maxSessionPerUser = MaxSessionPerUser;
    this.minSessionPerUser = MinSessionPerUser;
  }

  public ConfigPhysicalPlanType getAuthorType() {
    return super.getType();
  }

  public String getRoleName() {
    return roleName;
  }

  public void setRoleName(final String roleName) {
    this.roleName = roleName;
  }

  public String getPassword() {
    return password;
  }

  public int getMaxSessionPerUser() {
    return maxSessionPerUser;
  }

  public void setMaxSessionPerUser(final int maxSessionPerUser) {
    this.maxSessionPerUser = maxSessionPerUser;
  }

  public int getMinSessionPerUser() {
    return minSessionPerUser;
  }

  public void setMinSessionPerUser(final int minSessionPerUser) {
    this.maxSessionPerUser = minSessionPerUser;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public String getNewPassword() {
    return newPassword;
  }

  public void setNewPassword(String newPassword) {
    this.newPassword = newPassword;
  }

  public boolean getGrantOpt() {
    return this.grantOpt;
  }

  public void setGrantOpt(final boolean grantOpt) {
    this.grantOpt = grantOpt;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(final String userName) {
    this.userName = userName;
  }

  public String getNewUsername() {
    return newUsername;
  }

  public long getExecutedByUserId() {
    return executedByUserId;
  }

  public void setExecutedByUserId(long executedByUserId) {
    this.executedByUserId = executedByUserId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthorPlan)) {
      return false;
    }
    AuthorPlan that = (AuthorPlan) o;
    return Objects.equals(super.getType(), that.getAuthorType())
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(newPassword, that.newPassword)
        && grantOpt == that.grantOpt
        && Objects.equals(newUsername, that.newUsername);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.getType(), userName, roleName, password, newPassword, grantOpt, newUsername);
  }

  @Override
  public String toString() {
    return "[type:"
        + super.getType()
        + ", username:"
        + userName
        + ", rolename:"
        + roleName
        + ", grant option:"
        + grantOpt
        + ", new username:"
        + newUsername
        + "]";
  }
}
