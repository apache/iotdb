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
#pragma once
#include <cctype>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

inline std::vector<std::pair<std::string, std::string>>
ParseConnectionString(const std::string& text) {
  std::vector<std::pair<std::string, std::string>> values;
  size_t pos = 0;
  while (pos < text.size()) {
    while (pos < text.size() &&
           (text[pos] == ';' || std::isspace(static_cast<unsigned char>(text[pos]))))
      ++pos;
    if (pos == text.size())
      break;
    auto equal = text.find('=', pos);
    if (equal == std::string::npos || text.find(';', pos) < equal)
      throw std::invalid_argument("Invalid connection attribute");
    std::string key = text.substr(pos, equal - pos);
    while (!key.empty() && std::isspace(static_cast<unsigned char>(key.back())))
      key.pop_back();
    if (key.empty())
      throw std::invalid_argument("Empty connection attribute");
    pos = equal + 1;
    std::string value;
    if (pos < text.size() && text[pos] == '{') {
      ++pos;
      bool closed = false;
      while (pos < text.size()) {
        char c = text[pos++];
        if (c == '}') {
          if (pos < text.size() && text[pos] == '}')
            ++pos;
          else {
            closed = true;
            break;
          }
        }
        value += c;
      }
      if (!closed)
        throw std::invalid_argument("Unclosed connection attribute");
      while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos])))
        ++pos;
      if (pos < text.size() && text[pos] != ';')
        throw std::invalid_argument("Invalid braced connection attribute");
    } else {
      auto end = text.find(';', pos);
      if (end == std::string::npos)
        end = text.size();
      value = text.substr(pos, end - pos);
      pos = end;
    }
    values.emplace_back(std::move(key), std::move(value));
  }
  return values;
}
