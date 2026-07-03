/**
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

#ifndef IOTDB_IT_SSL_CONNECTION_H
#define IOTDB_IT_SSL_CONNECTION_H

#include "SessionC.h"

#ifdef __cplusplus
#include <memory>

#include "Session.h"
#include "SessionBuilder.h"
#include "SessionPool.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/** Apply one-way TLS settings for integration tests against a TLS-enabled IoTDB. */
void it_ssl_configure_tree_session(CSession* session);
void it_ssl_configure_table_session(CTableSession* session);

#ifdef __cplusplus
}

namespace itssl {

#if defined(WITH_SSL) && defined(IOTDB_RPC_SSL_IT)
void configureSessionBuilder(SessionBuilder& builder);
void configureTableSessionBuilder(TableSessionBuilder& builder);
void configureSessionPoolBuilder(SessionPoolBuilder& builder);
std::shared_ptr<Session> newOpenedTreeSession();
std::shared_ptr<TableSession> newOpenedTableSession();
#endif

} // namespace itssl
#endif

#endif // IOTDB_IT_SSL_CONNECTION_H
