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

#ifndef IOTDB_EXAMPLE_TLS_CONFIG_H
#define IOTDB_EXAMPLE_TLS_CONFIG_H

#include "SessionC.h"

#ifdef __cplusplus
#include "Session.h"
#include "SessionBuilder.h"
#include "TableSession.h"
#include "TableSessionBuilder.h"
#endif

#ifdef __cplusplus
extern "C" {
#endif

/** One-way TLS trust store used by TLS examples (PKCS12 under fixtures/tls/). */
const char* example_tls_trust_store_path(void);

void example_tls_configure_tree_session(CSession* session);
void example_tls_configure_table_session(CTableSession* session);

#ifdef __cplusplus
}

namespace examplessl {

void configureTreeSessionBuilder(SessionBuilder& builder);
void configureTableSessionBuilder(TableSessionBuilder& builder);

} // namespace examplessl
#endif

#endif // IOTDB_EXAMPLE_TLS_CONFIG_H
