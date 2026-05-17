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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.it.framework.IoTDBTestLogger;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class PipeEnvReuseManager {

  private static final Logger LOGGER = IoTDBTestLogger.logger;
  private static final Set<String> BUILTIN_USERS =
      new HashSet<>(
          Arrays.asList(
              "root", "sys_admin", "security_admin", "audit_admin", "__internal_auditor"));
  private static final Set<String> BUILTIN_TABLE_DATABASES =
      new HashSet<>(Arrays.asList("information_schema"));

  private PipeEnvReuseManager() {
    // Utility class
  }

  public static void prepareForNextTest(final BaseEnv... envs) {
    final List<BaseEnv> distinctEnvs =
        Arrays.stream(envs).filter(Objects::nonNull).distinct().collect(Collectors.toList());
    try {
      for (final BaseEnv env : distinctEnvs) {
        resetEnvironment(env);
      }
    } catch (final Exception e) {
      LOGGER.warn("Failed to logically reset pipe IT environment, fallback to full cleanup", e);
      for (final BaseEnv env : distinctEnvs) {
        try {
          env.cleanClusterEnvironment();
        } catch (final Exception cleanupException) {
          LOGGER.warn("Failed to fully cleanup pipe IT environment", cleanupException);
        }
      }
    }
  }

  private static void resetEnvironment(final BaseEnv env) throws SQLException {
    dropAllPipes(env);
    dropAllCustomPipePlugins(env);
    clearTreeDatabases(env);
    dropAllTemplates(env);
    clearUsers(env);
    clearRoles(env);
    clearTableDatabases(env);
  }

  private static void dropAllPipes(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show pipes")) {
      final List<String> pipeNames = new ArrayList<>();
      while (resultSet.next()) {
        final String pipeName = resultSet.getString(1);
        if (pipeName != null && !pipeName.startsWith("__consensus")) {
          pipeNames.add(pipeName);
        }
      }
      for (final String pipeName : pipeNames) {
        statement.execute("drop pipe " + quoteTreeIdentifier(pipeName));
      }
    }
  }

  private static void dropAllCustomPipePlugins(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show pipeplugins")) {
      final List<String> pluginNames = new ArrayList<>();
      while (resultSet.next()) {
        final String pluginName = resultSet.getString(1);
        final String pluginType = resultSet.getString(2);
        if (pluginName != null && !"Builtin".equalsIgnoreCase(pluginType)) {
          pluginNames.add(pluginName);
        }
      }
      for (final String pluginName : pluginNames) {
        statement.execute("drop pipePlugin " + quoteTreeIdentifier(pluginName));
      }
    }
  }

  private static void clearTreeDatabases(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement()) {
      try {
        statement.execute("delete database root.**");
      } catch (final SQLException ignored) {
        try {
          statement.execute("drop database root.**");
        } catch (final SQLException ignoredAgain) {
          // Ignore when there is no tree database to drop.
        }
      }
    }
  }

  private static void dropAllTemplates(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show device templates")) {
      final List<String> templateNames = new ArrayList<>();
      while (resultSet.next()) {
        final String templateName = resultSet.getString(1);
        if (templateName != null) {
          templateNames.add(templateName);
        }
      }
      for (final String templateName : templateNames) {
        statement.execute("drop device template " + quoteTreeIdentifier(templateName));
      }
    }
  }

  private static void clearUsers(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("list user")) {
      final List<String> users = new ArrayList<>();
      while (resultSet.next()) {
        final String user = resultSet.getString(1);
        if (user != null && !BUILTIN_USERS.contains(user.toLowerCase(Locale.ROOT))) {
          users.add(user);
        }
      }
      for (final String user : users) {
        statement.execute("drop user " + quoteTreeIdentifier(user));
      }
    }
  }

  private static void clearRoles(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("list role")) {
      final List<String> roles = new ArrayList<>();
      while (resultSet.next()) {
        final String role = resultSet.getString(1);
        if (role != null) {
          roles.add(role);
        }
      }
      for (final String role : roles) {
        statement.execute("drop role " + quoteTreeIdentifier(role));
      }
    }
  }

  private static void clearTableDatabases(final BaseEnv env) throws SQLException {
    try (final Connection connection = env.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("show databases")) {
      final List<String> databases = new ArrayList<>();
      while (resultSet.next()) {
        final String database = resultSet.getString(1);
        if (database != null
            && !BUILTIN_TABLE_DATABASES.contains(database.toLowerCase(Locale.ROOT))) {
          databases.add(database);
        }
      }
      for (final String database : databases) {
        statement.execute("drop database if exists " + quoteTableIdentifier(database));
      }
    }
  }

  private static String quoteTreeIdentifier(final String identifier) {
    return "`" + identifier.replace("`", "``") + "`";
  }

  private static String quoteTableIdentifier(final String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
