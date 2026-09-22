<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Deterministic migration verification

Verified tasks: 274; rows: 43,666,522.

No model was used. FS results include external generic deterministic computation.

| Family | Tasks | Storage | SQL/oracle | FS/oracle | Public/gold | Private/gold |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Global Aggregation | 72 | 72 | 72 | 72 | 67 | 72 |
| Interval Discovery | 72 | 72 | 72 | 72 | 60 | 72 |
| Temporal Localization | 72 | 72 | 72 | 72 | 72 | 72 |
| Sliding Window | 58 | 58 | 58 | 58 | 51 | 51 |

## Original gold discrepancies

Original questions and gold were not modified. Private bounds are diagnostic only.

| Task | Reason | Requested window days | Gold interval days | Public oracle | Original gold |
| --- | --- | ---: | ---: | --- | --- |
| L1_T1_Global_Aggregation_00059 | private_time_boundary |  |  | 0.7951545833333333 | 0.796 |
| L1_T1_Global_Aggregation_00070 | private_time_boundary |  |  | 76.0 | 74.0 |
| L1_T1_Global_Aggregation_00080 | private_time_boundary |  |  | 0.9672554347826086 | 0.964 |
| L1_T1_Global_Aggregation_00082 | private_time_boundary |  |  | 0.231 | 0.23 |
| L1_T1_Global_Aggregation_00147 | private_time_boundary |  |  | 0.779 | 0.778 |
| L1_T3_Interval_Discovery_00341 | private_time_boundary |  |  | ["2022-09-27 00:00:00", "2022-10-03 11:00:00"] | ["2022-09-27 05:45:00", "2022-10-03 11:00:00"] |
| L1_T3_Interval_Discovery_00346 | private_time_boundary |  |  | ["2020-03-31 00:00:00", "2020-04-03 17:30:00"] | ["2020-03-31 09:30:00", "2020-04-03 17:30:00"] |
| L1_T3_Interval_Discovery_00355 | private_time_boundary |  |  | ["2022-09-19 00:00:00", "2022-09-22 14:00:00"] | ["2022-09-19 17:30:00", "2022-09-22 14:00:00"] |
| L1_T3_Interval_Discovery_00365 | private_time_boundary |  |  | ["2023-11-19 21:45:00", "2023-12-05 23:45:00"] | ["2023-11-19 21:45:00", "2023-12-05 21:30:00"] |
| L1_T3_Interval_Discovery_00382 | private_time_boundary |  |  | ["2023-04-06 00:00:00", "2023-04-19 10:30:00"] | ["2023-04-06 19:30:00", "2023-04-19 10:30:00"] |
| L1_T3_Interval_Discovery_00420 | private_time_boundary |  |  | ["2022-11-25 14:00:00", "2022-12-03 23:45:00"] | ["2022-11-25 14:00:00", "2022-12-03 17:15:00"] |
| L1_T3_Interval_Discovery_00424 | private_time_boundary |  |  | ["2021-09-29 00:00:00", "2021-10-04 20:15:00"] | ["2021-09-29 19:15:00", "2021-10-04 20:15:00"] |
| L1_T3_Interval_Discovery_00436 | private_time_boundary |  |  | ["2019-03-31 00:00:00", "2019-04-07 07:45:00"] | ["2019-03-31 03:15:00", "2019-04-07 07:45:00"] |
| L1_T3_Interval_Discovery_00465 | private_time_boundary |  |  | ["2020-12-21 05:00:00", "2020-12-29 23:45:00"] | ["2020-12-21 05:00:00", "2020-12-29 21:15:00"] |
| L1_T3_Interval_Discovery_00474 | private_time_boundary |  |  | ["2020-07-25 00:00:00", "2020-08-01 12:45:00"] | ["2020-07-25 00:45:00", "2020-08-01 12:45:00"] |
| L1_T3_Interval_Discovery_00488 | private_time_boundary |  |  | ["2020-05-26 00:00:00", "2020-05-28 22:15:00"] | ["2020-05-26 12:45:00", "2020-05-28 22:15:00"] |
| L1_T3_Interval_Discovery_00492 | private_time_boundary |  |  | ["2023-04-30 00:00:00", "2023-05-05 15:30:00"] | ["2023-04-30 11:30:00", "2023-05-05 15:30:00"] |
| L1_T4_Sliding_Window_00531 | incomplete_gold_window | 49 | 42.34375 | ["2019-01-01 00:00:00", "2019-02-18 23:45:00"] | ["2019-01-01 00:00:00", "2019-02-12 08:15:00"] |
| L1_T4_Sliding_Window_00533 | incomplete_gold_window | 58 | 0.0 | ["2021-06-15 18:00:00", "2021-08-12 17:45:00"] | ["2021-01-01 00:00:00", "2021-01-01 00:00:00"] |
| L1_T4_Sliding_Window_00627 | incomplete_gold_window | 41 | 1.3229166666666667 | ["2018-05-06 11:45:00", "2018-06-16 11:30:00"] | ["2018-01-01 00:00:00", "2018-01-02 07:45:00"] |
| L1_T4_Sliding_Window_00642 | incomplete_gold_window | 18 | 0.46875 | ["2021-02-04 00:30:00", "2021-02-22 00:15:00"] | ["2021-01-01 00:00:00", "2021-01-01 11:15:00"] |
| L1_T4_Sliding_Window_00656 | incomplete_gold_window | 60 | 55.677083333333336 | ["2022-01-01 00:00:00", "2022-03-01 23:45:00"] | ["2022-01-01 00:00:00", "2022-02-25 16:15:00"] |
| L1_T4_Sliding_Window_00681 | incomplete_gold_window | 28 | 0.10416666666666667 | ["2018-01-13 23:15:00", "2018-02-10 23:00:00"] | ["2018-01-01 00:00:00", "2018-01-01 02:30:00"] |
| L1_T4_Sliding_Window_00689 | incomplete_gold_window | 4 | 0.23333333333333334 | ["2020-01-10 00:48:00", "2020-01-14 00:47:00"] | ["2020-01-01 00:00:00", "2020-01-01 05:36:00"] |

## Interface discrepancies

| Task | SQL | FS composition | Oracle |
| --- | --- | --- | --- |

None under the declared public answer contract.
