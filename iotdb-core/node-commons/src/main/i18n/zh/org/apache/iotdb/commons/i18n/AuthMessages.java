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

package org.apache.iotdb.commons.i18n;

public final class AuthMessages {

  // ======================== BasicAuthorizer ========================

  public static final String AUTHORIZER_INIT_COMPLETE = "权限管理器初始化完成";
  public static final String AUTHORIZER_UNINITIALIZED = "权限管理器未初始化";
  public static final String AUTHORIZER_PROVIDER_CLASS = "权限管理器提供类: {}";
  public static final String AUTHORIZER_INIT_FAILED = "权限管理器初始化失败！";

  public static final String NO_SUCH_ROLE = "角色不存在: %s";
  public static final String NO_SUCH_USER = "用户不存在: %s";

  public static final String USER_NOT_EXIST = "用户 %s 不存在。";
  public static final String INCORRECT_PASSWORD = "密码错误。";
  public static final String USER_ALREADY_EXISTS = "用户 %s 已存在";
  public static final String USER_DOES_NOT_EXIST = "用户 %s 不存在";
  public static final String ROLE_ALREADY_EXISTS = "角色 %s 已存在";
  public static final String ROLE_DOES_NOT_EXIST = "角色 %s 不存在";

  public static final String ADMIN_CANNOT_BE_DELETED = "默认管理员不可删除";
  public static final String ADMIN_ALREADY_HAS_ALL_PRIVILEGES = "无效操作，管理员已拥有所有权限";
  public static final String ADMIN_MUST_HAVE_ALL_PRIVILEGES = "无效操作，管理员必须拥有所有权限";
  public static final String ADMIN_CANNOT_REVOKE_PRIVILEGES = "无效操作，管理员不可撤销权限";
  public static final String CANNOT_GRANT_ROLE_TO_ADMIN = "无效操作，不可授予管理员角色";
  public static final String CANNOT_REVOKE_ROLE_FROM_ADMIN = "无效操作，不可撤销管理员的角色 ";

  public static final String REVOKE_ROLE_FROM_USER_ERROR = "删除角色后，从用户 {} 撤销角色 {} 时发生错误";

  public static final String PASSWORD_IS_ILLEGAL = "密码 %s 不合法";

  public static final String GET_ALL_USERS_FAILED = "获取所有用户失败，用户不存在: {}";
  public static final String GET_ALL_ROLES_FAILED = "获取所有角色失败，角色不存在: {}";

  public static final String ROLE_ALREADY_EXISTS_LOG = "角色 {} 已存在";

  // ======================== BasicRoleManager ========================

  public static final String NO_SUCH_ROLE_FMT = "角色 %s 不存在";
  public static final String NO_SUCH_USER_FMT = "用户 %s 不存在";

  public static final String NOT_SUPPORT_MODEL_TYPE = "不支持的模型类型 {}";

  public static final String LOAD_ROLE_EXCEPTION = "加载角色 {} 时发生异常";

  // ======================== BasicUserManager ========================

  public static final String WRONG_PATH_INIT = "初始化 {} 时路径错误";
  public static final String INTERNAL_USER_INITIALIZED = "内部用户 {} 已初始化";
  public static final String LOAD_MAX_USER_ID_ERROR = "加载最大用户 ID 时出错。";
  public static final String PASSWORD_SAME_AS_USERNAME = "密码不可与用户名相同";
  public static final String BUILTIN_USERNAME_IN_USE = "管理员内置用户名已被使用";
  public static final String LOAD_USER_EXCEPTION = "加载用户 {} 时发生异常";

  public static final String RENAME_USER_TARGET_EXISTS = "无法将用户 %s 重命名为 %s，目标用户名已存在。";

  // ======================== LocalFileRoleAccessor ========================

  public static final String NEW_PROFILE_RENAME_FAILED = "新配置文件重命名失败。";
  public static final String FAILED_TO_CREATE_ROLE_DIR = "创建角色目录 {} 失败";
  public static final String SAVE_ROLE_ERROR = "保存角色 {} 时出错";
  public static final String CANNOT_DELETE_ROLE_FILE = "无法删除角色文件: %s";
  public static final String FAILED_TO_LOAD_ROLE_SNAPSHOT = "加载角色目录快照失败，正在回滚。";
  public static final String NO_ROLES_TO_LOAD = "没有需要加载的角色。";
  public static final String ROLE_INFO_DIR_CREATED = "角色信息目录 {} 已创建";
  public static final String ROLE_INFO_DIR_CREATE_FAILED = "角色信息目录 {} 创建失败";
  public static final String MOVE_OLD_ROLE_DIR_FAILED = "移动旧角色目录失败: {}";
  public static final String ROLE_FOLDER_NOT_EXISTS = "角色目录不存在";
  public static final String FAILED_TO_CREATE_USER_DIR = "创建用户目录 {} 失败";
  public static final String SAVE_USER_ID_ERROR = "保存用户 ID {} 时出错";

  // ======================== LocalFileUserAccessor ========================

  public static final String SAVE_USER_ROLES_ERROR = "保存用户 {} 的角色时发生异常";
  public static final String FAILED_TO_LOAD_USER_SNAPSHOT = "加载用户目录快照失败，正在回滚。";
  public static final String CATCH_ERROR_DELETE_USER_ROLE = "删除用户 %s 的角色时出错";

  // ======================== PrivilegeType ========================

  public static final String UNEXPECTED_PRIVILEGE_VALUE = "意外的权限值:";

  private AuthMessages() {}

  public static final String NAME_CANNOT_CONTAIN_SPACES = "名称不能包含空格";
  public static final String PASSWORD_CANNOT_CONTAIN_SPACES = "密码不能包含空格";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_ACCESS_DENIED_E128C1A1 = "访问被拒绝：";
  public static final String EXCEPTION_ENCRYPT_DECRYPT_FAILED_163764B5 = "加密或解密失败。";
  public static final String EXCEPTION_BACKSLASH_QUOTE_D4C5D637 = "\"";

}
