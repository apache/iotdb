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

package org.apache.iotdb.postgresql.i18n;

/** Log and exception messages of the PostgreSQL service (en). */
public final class PostgreSqlMessages {

  private PostgreSqlMessages() {}

  public static final String EXCEPTION_ARG_ARG_IS_NOT_A_LOOPBACK_ADDRESS_AND_NEEDS_TLS_BUT_NO_KEY_STORE_IS_CONFIGURED_KEY_STORE_PATH_723B55BF =
      "%s=%s is not a loopback address and needs TLS, but no key store is configured (key_store_path)";

  public static final String EXCEPTION_BINARY_FORMAT_OF_TYPE_ARG_IS_NOT_SUPPORTED_116397E1 =
      "binary format of type %s is not supported";

  public static final String EXCEPTION_CANNOT_INSPECT_ARG_A8699317 =
      "cannot inspect %s";

  public static final String EXCEPTION_CANNOT_OPEN_ARG_ARG_ARG_ARG_63428D45 =
      "cannot open %s:%s (%s): %s";

  public static final String EXCEPTION_INTERRUPTED_WHILE_OPENING_ARG_ARG_F80AAF97 =
      "interrupted while opening %s:%s";

  public static final String EXCEPTION_INVALID_SETTINGS_ARG_4D6B78F5 =
      "invalid settings: %s";

  public static final String EXCEPTION_INVALID_VALUE_FOR_PARAMETER_STATEMENT_TIMEOUT_ARG_30718625 =
      "invalid value for parameter \"statement_timeout\": \"%s\"";

  public static final String EXCEPTION_NOTHING_IS_WRITTEN_WHILE_SKIPPING_5DE21CBD =
      "nothing is written while skipping";

  public static final String EXCEPTION_NOT_A_BOOLEAN_ARG_BDDA84BC =
      "not a boolean: %s";

  public static final String EXCEPTION_NO_BINARY_FORMAT_FOR_ARG_62465CF6 =
      "No binary format for %s";

  public static final String EXCEPTION_NO_TEXT_FORMAT_FOR_ARG_F368DE42 =
      "No text format for %s";

  public static final String EXCEPTION_ONLY_THE_HEX_FORMAT_OF_BYTEA_IS_SUPPORTED_86198CC1 =
      "only the hex format of bytea is supported";

  public static final String EXCEPTION_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_A5563154 =
      "PostgreSQL endpoint not started: %s";

  public static final String EXCEPTION_THE_JVM_SUPPORTS_NONE_OF_THE_ALLOWED_TLS_PROTOCOLS_ARG_OR_CIPHER_SUITES_833995C7 =
      "The JVM supports none of the allowed TLS protocols %s or cipher suites";

  public static final String EXCEPTION_THE_KEY_STORE_ARG_CANNOT_BE_USED_ARG_5908F0BB =
      "the key store %s cannot be used: %s";

  public static final String EXCEPTION_THE_KEY_STORE_ARG_IS_NOT_READABLE_A4A4A28C =
      "the key store %s is not readable";

  public static final String LOG_POSTGRESQL_CLIENT_ARG_DISCONNECTED_WHILE_RESULTS_WERE_SENT_F0C48BEA =
      "PostgreSQL client {} disconnected while results were sent";

  public static final String LOG_POSTGRESQL_CLIENT_ARG_STOPPED_READING_RESULTS_FOR_ARG_CLOSING_THE_CONNECTION_0A30373B =
      "PostgreSQL client {} stopped reading results for {}; closing the connection";

  public static final String LOG_POSTGRESQL_CONNECTION_ARG_FAILED_C649F2E0 =
      "PostgreSQL connection {} failed";

  public static final String LOG_POSTGRESQL_ENDPOINT_CANNOT_READ_ARG_ARG_B4423CA2 =
      "PostgreSQL endpoint: cannot read {}: {}";

  public static final String LOG_POSTGRESQL_ENDPOINT_LISTENING_ON_ARG_9DBA69A9 =
      "PostgreSQL endpoint listening on {}";

  public static final String LOG_POSTGRESQL_ENDPOINT_NOT_STARTED_ARG_626B180C =
      "PostgreSQL endpoint not started: {}";

  public static final String LOG_POSTGRESQL_LOGIN_FAILED_7130B38D =
      "PostgreSQL login failed";

  public static final String LOG_POSTGRESQL_MESSAGE_ARG_OF_ARG_FAILED_EB758E37 =
      "PostgreSQL message '{}' of {} failed";

  public static final String LOG_POSTGRESQL_SESSION_OF_ARG_WAS_ENDED_CLOSING_THE_CONNECTION_85AD9B1D =
      "PostgreSQL session of {} was ended; closing the connection";

  public static final String LOG_REFUSED_POSTGRESQL_STATEMENT_ARG_ARG_602B9945 =
      "Refused PostgreSQL statement ({}): {}";

  public static final String LOG_THE_DATABASE_ARG_IS_HIDDEN_FROM_POSTGRESQL_CLIENTS_ITS_NAME_IS_A_RESERVED_POSTGRESQL_SCHEMA_B92A6ECA =
      "The database {} is hidden from PostgreSQL clients: its name is a reserved PostgreSQL schema";

  public static final String LOG_UNKNOWN_POSTGRESQL_PARAMETER_ARG_ACCEPTED_AND_IGNORED_C2408108 =
      "Unknown PostgreSQL parameter {} accepted and ignored";
}
