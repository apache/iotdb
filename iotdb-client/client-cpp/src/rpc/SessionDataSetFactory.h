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
#ifndef IOTDB_SESSION_DATA_SET_FACTORY_H
#define IOTDB_SESSION_DATA_SET_FACTORY_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "IClientRPCService.h"

class SessionDataSet;

std::unique_ptr<SessionDataSet> createSessionDataSet(
    const std::string &sql, const std::vector<std::string> &columnNameList,
    const std::vector<std::string> &columnTypeList,
    const std::map<std::string, int32_t> &columnNameIndex, int64_t queryId,
    int64_t statementId, std::shared_ptr<IClientRPCServiceClient> client,
    int64_t sessionId, const std::vector<std::string> &queryResult,
    bool ignoreTimestamp, int64_t timeout, bool moreData, int32_t fetchSize,
    const std::string &zoneId);

#endif
