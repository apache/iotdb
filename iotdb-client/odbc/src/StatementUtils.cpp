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
#include "StatementUtils.h"

#include <algorithm>
#include <cctype>

bool IsQueryStatement(const std::string& sql) {
  size_t start = 0;
  while ((start = sql.find_first_not_of(" \t\n\r", start)) != std::string::npos) {
    if (sql.compare(start, 2, "--") == 0) {
      start = sql.find_first_of("\n\r", start + 2);
      if (start == std::string::npos)
        return false;
    } else if (sql.compare(start, 2, "/*") == 0) {
      start = sql.find("*/", start + 2);
      if (start == std::string::npos)
        return false;
      start += 2;
    } else {
      break;
    }
  }
  if (start == std::string::npos)
    return false;

  size_t end = start;
  while (end < sql.size() &&
         (std::isalnum(static_cast<unsigned char>(sql[end])) || sql[end] == '_'))
    ++end;
  std::string keyword = sql.substr(start, end == std::string::npos ? end : end - start);
  std::transform(keyword.begin(), keyword.end(), keyword.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return keyword == "select" || keyword == "with" || keyword == "show" || keyword == "describe" ||
         keyword == "desc" || keyword == "list";
}

std::vector<size_t> ParameterMarkerPositions(const std::string& sql) {
  std::vector<size_t> result;
  char quote = '\0';
  bool lineComment = false;
  bool blockComment = false;
  for (size_t i = 0; i < sql.size(); ++i) {
    const char current = sql[i];
    const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';
    if (lineComment) {
      if (current == '\n' || current == '\r')
        lineComment = false;
      continue;
    }
    if (blockComment) {
      if (current == '*' && next == '/') {
        blockComment = false;
        ++i;
      }
      continue;
    }
    if (quote) {
      if (current == quote) {
        if (next == quote)
          ++i;
        else
          quote = '\0';
      }
      continue;
    }
    if (current == '-' && next == '-') {
      lineComment = true;
      ++i;
      continue;
    }
    if (current == '/' && next == '*') {
      blockComment = true;
      ++i;
      continue;
    }
    if (current == '\'' || current == '"' || current == '`') {
      quote = current;
      continue;
    }
    if (current == '?')
      result.push_back(i);
  }
  return result;
}
