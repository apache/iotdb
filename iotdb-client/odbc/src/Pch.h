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
#ifndef PCH_H
#define PCH_H

// Common preprocessor definitions
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN // Exclude unnecessary Windows API
#endif

// Platform-specific includes
#if (defined(_WIN32) || defined(__WIN32__)) && !defined(WIN32)
#define WIN32
#endif

#ifdef WIN32
#include <windows.h>
#endif

// ODBC includes
#include <sql.h>
#include <sqlext.h>

// Third-party libraries
#include <nlohmann/json.hpp>

// Boost includes
#include <Optional.h>
#include <Date.h>
#include <climits>
#include <cstdint>
#include <cstring>
#include <limits>
#include <ctime>
#include <codecvt>
#include <locale>

// Custom boost::throw_exception implementation when BOOST_NO_EXCEPTIONS is defined

// Standard library includes
#include <iostream>
#include <iterator>
#include <vector>
#include <string>
#include <memory>
#include <deque>
#include <mutex>
#include <sstream>
#include <functional>
#include <algorithm>

#endif // PCH_H
