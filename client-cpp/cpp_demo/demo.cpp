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

#include "Session.h" // from iotdb_session_sdk
#include <string>
#include <vector>
#include <sstream>

void print_sizeof()
{
  std::cout << "sizeof(bool) = " << sizeof(bool) << std::endl;
  std::cout << "sizeof(char) = " << sizeof(char) << std::endl;
  std::cout << "sizeof(int) = " << sizeof(int) << std::endl;
  std::cout << "sizeof(long) = " << sizeof(long) << std::endl;
  std::cout << "sizeof(void*) = " << sizeof(void*) << std::endl;
  std::cout << "sizeof(int32_t) = " << sizeof(int32_t) << std::endl;
  std::cout << "sizeof(int64_t) = " << sizeof(int64_t) << std::endl;
}

int64_t total_row_count = 0;
char cmd = 'a';
int max_row_num = 5;
int measurement_num = 3;


const char* parse_ev(const char *ev_name)
{
  const char *ev = getenv(ev_name);
  if (ev == nullptr) {
    printf("you need specify environment variable: %s\n", ev_name);
    exit(1);
  }
  return ev;
}

void parse_param()
{
  printf("get param from enviroment variables: DEMO__total_row_count, "
      "DEMO__cmd, DEMO__max_row_num, DEMO__measurement_num\n");

  total_row_count = atol(parse_ev("DEMO__total_row_count"));
  cmd = parse_ev("DEMO__cmd")[0];
  max_row_num = atoi(parse_ev("DEMO__max_row_num"));
  measurement_num = atoi(parse_ev("DEMO__measurement_num"));

  printf("total_row_count=%lld, cmd=%c, max_row_num=%d, measurement_num=%d\n",
      total_row_count, cmd, max_row_num, measurement_num);
}

int64_t get_ms()
{
  struct timeval tp;
  gettimeofday(&tp, NULL);
  return tp.tv_sec * 1000 + tp.tv_usec / 1000;
}

int main(int argc, char **args)
{
  print_sizeof();

  parse_param();

  const char *device_id = "root.sg01.d01";
  Session *session = new Session("127.0.0.1", 6667, "root", "root");
  session->open(false);

  std::vector<std::pair<std::string, TSDataType::TSDataType>> schemas;
  for (int i = 0; i < measurement_num; i++) {
    char buf[16];
    sprintf(buf, "sensor_%d", i);
    std::pair<std::string, TSDataType::TSDataType> p(std::string(buf), TSDataType::INT64);
    schemas.push_back(p);
  }

  Tablet tablet(device_id, schemas, max_row_num);

  int64_t timestamp = 0;

  if (cmd == 'a' || cmd == 'i') {
    int64_t start_ms = get_ms();
    for (int row = 0; row < total_row_count; row++) {
      int64_t rowIndex = tablet.rowSize++;
      tablet.timestamps[rowIndex] = timestamp;
      for (int s = 0; s < measurement_num; s++) {
        int64_t value = (row + 1) * 100 + s + 1;
        tablet.addValue(s, rowIndex, &value);
      }
      if (tablet.rowSize >= max_row_num) {
        session->insertTablet(tablet, /* sorted = */ true);
        tablet.reset();
      }
      timestamp++;
    }
    if (tablet.rowSize > 0) {
      session->insertTablet(tablet, /* sorted = */ true);
      tablet.reset();
    }
    int64_t end_ms = get_ms();
    printf("== PERF == insert %lld rows done, time spent = %lld ms.\n", total_row_count, end_ms - start_ms);
  }

  char buf[256];
  size_t buf_sz = 256;
  if (cmd == 'a' || cmd == 's') {
    snprintf(buf, buf_sz, "select * from %s ;", device_id);
    std::unique_ptr<SessionDataSet> res = session->executeQueryStatement(buf);
    printf("execute %s done, res ptr=%p\n", buf, res.get());
    while (res->hasNext()) {
      std::cout << "--- RES: " << res->next()->toString() << std::endl;
    }
  }

  if (cmd == 'a' || cmd == 'c') {
    snprintf(buf, buf_sz, "select count(*) from %s ;", device_id);
    int64_t start_ms = get_ms();
    std::unique_ptr<SessionDataSet> res = session->executeQueryStatement(buf);
    int64_t end_ms = get_ms();
    printf("== PERF == execute %s done, time spent = %lld\n", buf, end_ms - start_ms);
    while (res->hasNext()) {
      std::cout << "--- RES: " << res->next()->toString() << std::endl;
    }
  }

  session->close();
}

