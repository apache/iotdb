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

#ifndef IOTDB_TS_BLOCK_H
#define IOTDB_TS_BLOCK_H

#include <vector>
#include <memory>
#include "Column.h"

class TsBlock {
public:
    static std::shared_ptr<TsBlock> create(int32_t positionCount,
                                           std::shared_ptr<Column> timeColumn,
                                           std::vector<std::shared_ptr<Column>> valueColumns);

    static std::shared_ptr<TsBlock> deserialize(const std::string& data);

    int32_t getPositionCount() const;
    int64_t getStartTime() const;
    int64_t getEndTime() const;
    bool isEmpty() const;
    int64_t getTimeByIndex(int32_t index) const;
    int32_t getValueColumnCount() const;
    const std::shared_ptr<Column> getTimeColumn() const;
    const std::vector<std::shared_ptr<Column>>& getValueColumns() const;
    const std::shared_ptr<Column> getColumn(int32_t columnIndex) const;

private:
    TsBlock(int32_t positionCount,
            std::shared_ptr<Column> timeColumn,
            std::vector<std::shared_ptr<Column>> valueColumns);

    std::shared_ptr<Column> timeColumn_;
    std::vector<std::shared_ptr<Column>> valueColumns_;
    int32_t positionCount_;
};

#endif
