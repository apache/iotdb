<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# RegexMatch

## Usage

The function is used to fetch matched contents from text with given regular expression.

**Name:** REGEXMATCH

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `regex`: The regular expression to match in the text. All grammars supported by Java are acceptable, 
  for example, `\d+\.\d+\.\d+\.\d+` is expected to match any IPv4 addresses.
+ `group`: The wanted group index in the matched result. 
  Reference to java.util.regex, group 0 is the whole pattern and 
  the next ones are numbered with the appearance order of left parentheses. 
  For example, the groups in `A(B(CD))` are: 0-`A(B(CD))`, 1-`B(CD)`, 2-`CD`. 

**Output Series:** Output a single series. The type is TEXT.

**Note:** Those points with null values or not matched with the given pattern will not return any results.

## Examples

Input series:

```
+-----------------------------+-------------------------------+
|                         Time|                root.test.d1.s1|
+-----------------------------+-------------------------------+
|2021-01-01T00:00:01.000+08:00|        [192.168.0.1] [SUCCESS]|
|2021-01-01T00:00:02.000+08:00|       [192.168.0.24] [SUCCESS]|
|2021-01-01T00:00:03.000+08:00|           [192.168.0.2] [FAIL]|
|2021-01-01T00:00:04.000+08:00|        [192.168.0.5] [SUCCESS]|
|2021-01-01T00:00:05.000+08:00|      [192.168.0.124] [SUCCESS]|
+-----------------------------+-------------------------------+
```

SQL for query:

```sql
select regexmatch(s1, "regex"="\d+\.\d+\.\d+\.\d+", "group"="0") from root.test.d1
```

Output series:

```
+-----------------------------+----------------------------------------------------------------------+
|                         Time|regexmatch(root.test.d1.s1, "regex"="\d+\.\d+\.\d+\.\d+", "group"="0")|
+-----------------------------+----------------------------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                                           192.168.0.1|
|2021-01-01T00:00:02.000+08:00|                                                          192.168.0.24|
|2021-01-01T00:00:03.000+08:00|                                                           192.168.0.2|
|2021-01-01T00:00:04.000+08:00|                                                           192.168.0.5|
|2021-01-01T00:00:05.000+08:00|                                                         192.168.0.124|
+-----------------------------+----------------------------------------------------------------------+
```
