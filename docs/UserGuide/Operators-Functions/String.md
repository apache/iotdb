<!--

​    Licensed to the Apache Software Foundation (ASF) under one
​    or more contributor license agreements.  See the NOTICE file
​    distributed with this work for additional information
​    regarding copyright ownership.  The ASF licenses this file
​    to you under the Apache License, Version 2.0 (the
​    "License"); you may not use this file except in compliance
​    with the License.  You may obtain a copy of the License at
​    
​        http://www.apache.org/licenses/LICENSE-2.0
​    
​    Unless required by applicable law or agreed to in writing,
​    software distributed under the License is distributed on an
​    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
​    KIND, either express or implied.  See the License for the
​    specific language governing permissions and limitations
​    under the License.

-->

# String Processing

## STRING_CONTAINS

### Function introduction

This function checks whether the substring `s` exists in the string

**Function name:** STRING_CONTAINS

**Input sequence:** Only a single input sequence is supported, the type is TEXT.

**parameter:**
+ `s`: The string to search for.

**Output Sequence:** Output a single sequence, the type is BOOLEAN.

### Usage example

```   sql
select s1, string_contains(s1, 's'='warn') from root.sg1.d4;
```

``` 
+-----------------------------+--------------+-------------------------------------------+
|                         Time|root.sg1.d4.s1|string_contains(root.sg1.d4.s1, "s"="warn")|
+-----------------------------+--------------+-------------------------------------------+
|1970-01-01T08:00:00.001+08:00|    warn:-8721|                                       true|
|1970-01-01T08:00:00.002+08:00|  error:-37229|                                      false|
|1970-01-01T08:00:00.003+08:00|     warn:1731|                                       true|
+-----------------------------+--------------+-------------------------------------------+
Total line number = 3
It costs 0.007s
```

## STRING_MATCHES

### Function introduction

This function judges whether a string can be matched by the regular expression `regex`.

**Function name:** STRING_MATCHES

**Input sequence:** Only a single input sequence is supported, the type is TEXT.

**parameter:**
+ `regex`: Java standard library-style regular expressions.

**Output Sequence:** Output a single sequence, the type is BOOLEAN.

### Usage example

```sql
select s1, string_matches(s1, 'regex'='[^\\s]+37229') from root.sg1.d4;
```

```
+-----------------------------+--------------+------------------------------------------------------+
|                         Time|root.sg1.d4.s1|string_matches(root.sg1.d4.s1, "regex"="[^\\s]+37229")|
+-----------------------------+--------------+------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|    warn:-8721|                                                 false|
|1970-01-01T08:00:00.002+08:00|  error:-37229|                                                  true|
|1970-01-01T08:00:00.003+08:00|     warn:1731|                                                 false|
+-----------------------------+--------------+------------------------------------------------------+
Total line number = 3
It costs 0.007s
```

## Length

### Usage

The function is used to get the length of input series.

**Name:** LENGTH

**Input Series:** Only support a single input series. The data type is TEXT.

**Output Series:** Output a single series. The type is INT32.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, length(s1) from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+----------------------+
|                         Time|root.sg1.d1.s1|length(root.sg1.d1.s1)|
+-----------------------------+--------------+----------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                     6|
|1970-01-01T08:00:00.002+08:00|      22test22|                     8|
+-----------------------------+--------------+----------------------+
```

## Locate

### Usage

The function is used to get the position of the first occurrence of substring `target` in input series. Returns -1 if there are no `target` in input.

**Name:** LOCATE

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `target`: The substring to be located.
+ `reverse`: Indicates whether reverse locate is required. The default value is `false`, means left-to-right locate.

**Output Series:** Output a single series. The type is INT32.

**Note:** The index begins from 0.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, locate(s1, "target"="1") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+------------------------------------+
|                         Time|root.sg1.d1.s1|locate(root.sg1.d1.s1, "target"="1")|
+-----------------------------+--------------+------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                                   0|
|1970-01-01T08:00:00.002+08:00|      22test22|                                  -1|
+-----------------------------+--------------+------------------------------------+
```

Another SQL for query:

```sql
select s1, locate(s1, "target"="1", "reverse"="true") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+------------------------------------------------------+
|                         Time|root.sg1.d1.s1|locate(root.sg1.d1.s1, "target"="1", "reverse"="true")|
+-----------------------------+--------------+------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                                                     5|
|1970-01-01T08:00:00.002+08:00|      22test22|                                                    -1|
+-----------------------------+--------------+------------------------------------------------------+
```

## StartsWith

### Usage

The function is used to check whether input series starts with the specified prefix.

**Name:** STARTSWITH

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**
+ `target`: The prefix to be checked.

**Output Series:** Output a single series. The type is BOOLEAN.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, startswith(s1, "target"="1") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+----------------------------------------+
|                         Time|root.sg1.d1.s1|startswith(root.sg1.d1.s1, "target"="1")|
+-----------------------------+--------------+----------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                                    true|
|1970-01-01T08:00:00.002+08:00|      22test22|                                   false|
+-----------------------------+--------------+----------------------------------------+
```

## EndsWith

### Usage

The function is used to check whether input series ends with the specified suffix.

**Name:** ENDSWITH

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**
+ `target`: The suffix to be checked.

**Output Series:** Output a single series. The type is BOOLEAN.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, endswith(s1, "target"="1") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+--------------------------------------+
|                         Time|root.sg1.d1.s1|endswith(root.sg1.d1.s1, "target"="1")|
+-----------------------------+--------------+--------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                                  true|
|1970-01-01T08:00:00.002+08:00|      22test22|                                 false|
+-----------------------------+--------------+--------------------------------------+
```

## Concat

### Usage

The function is used to concat input series and target strings.

**Name:** CONCAT

**Input Series:** At least one input series. The data type is TEXT.

**Parameter:**
+ `targets`: A series of K-V, key needs to start with `target` and be not duplicated, value is the string you want to concat.
+ `series_behind`: Indicates whether series behind targets. The default value is `false`.

**Output Series:** Output a single series. The type is TEXT.

**Note:** 
+ If value of input series is NULL, it will be skipped.
+ We can only concat input series and `targets` separately. `concat(s1, "target1"="IoT", s2, "target2"="DB")` and
  `concat(s1, s2, "target1"="IoT", "target2"="DB")` gives the same result.

### Examples

Input series:

```
+-----------------------------+--------------+--------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|
+-----------------------------+--------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|          null|
|1970-01-01T08:00:00.002+08:00|      22test22|      2222test|
+-----------------------------+--------------+--------------+
```

SQL for query:

```sql
select s1, s2, concat(s1, s2, "target1"="IoT", "target2"="DB") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|concat(root.sg1.d1.s1, root.sg1.d1.s2, "target1"="IoT", "target2"="DB")|
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|          null|                                                            1test1IoTDB|
|1970-01-01T08:00:00.002+08:00|      22test22|      2222test|                                                  22test222222testIoTDB|
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------+
```

Another SQL for query:

```sql
select s1, s2, concat(s1, s2, "target1"="IoT", "target2"="DB", "series_behind"="true") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|concat(root.sg1.d1.s1, root.sg1.d1.s2, "target1"="IoT", "target2"="DB", "series_behind"="true")|
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|          null|                                                                                    IoTDB1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|      2222test|                                                                          IoTDB22test222222test|
+-----------------------------+--------------+--------------+-----------------------------------------------------------------------------------------------+
```

## Substr

### Usage

The function is used to get the substring `start` to `end - 1`. 

**Name:** SUBSTR

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**
+ `start`: Indicates the start position of substring.
+ `end`: Indicates the end position of substring.

**Output Series:** Output a single series. The type is TEXT.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, substr(s1, "start"="0", "end"="2") from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+----------------------------------------------+
|                         Time|root.sg1.d1.s1|substr(root.sg1.d1.s1, "start"="0", "end"="2")|
+-----------------------------+--------------+----------------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|                                            1t|
|1970-01-01T08:00:00.002+08:00|      22test22|                                            22|
+-----------------------------+--------------+----------------------------------------------+
```

## Upper

### Usage

The function is used to get the string of input series with all characters changed to uppercase.

**Name:** UPPER

**Input Series:** Only support a single input series. The data type is TEXT.

**Output Series:** Output a single series. The type is TEXT.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|
|1970-01-01T08:00:00.002+08:00|      22test22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, upper(s1) from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+---------------------+
|                         Time|root.sg1.d1.s1|upper(root.sg1.d1.s1)|
+-----------------------------+--------------+---------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|               1TEST1|
|1970-01-01T08:00:00.002+08:00|      22test22|             22TEST22|
+-----------------------------+--------------+---------------------+
```

## Lower

### Usage

The function is used to get the string of input series with all characters changed to lowercase.

**Name:** LOWER

**Input Series:** Only support a single input series. The data type is TEXT.

**Output Series:** Output a single series. The type is TEXT.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s1|
+-----------------------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1TEST1|
|1970-01-01T08:00:00.002+08:00|      22TEST22|
+-----------------------------+--------------+
```

SQL for query:

```sql
select s1, lower(s1) from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+---------------------+
|                         Time|root.sg1.d1.s1|lower(root.sg1.d1.s1)|
+-----------------------------+--------------+---------------------+
|1970-01-01T08:00:00.001+08:00|        1TEST1|               1test1|
|1970-01-01T08:00:00.002+08:00|      22TEST22|             22test22|
+-----------------------------+--------------+---------------------+
```

## Trim

### Usage

The function is used to get the string whose value is same to input series, with all leading and trailing space removed.

**Name:** TRIM

**Input Series:** Only support a single input series. The data type is TEXT.

**Output Series:** Output a single series. The type is TEXT.

**Note:** Returns NULL if input is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+
|                         Time|root.sg1.d1.s3|
+-----------------------------+--------------+
|1970-01-01T08:00:00.002+08:00|   3querytest3|
|1970-01-01T08:00:00.003+08:00|  3querytest3 |
+-----------------------------+--------------+
```

SQL for query:

```sql
select s3, trim(s3) from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+--------------------+
|                         Time|root.sg1.d1.s3|trim(root.sg1.d1.s3)|
+-----------------------------+--------------+--------------------+
|1970-01-01T08:00:00.002+08:00|   3querytest3|         3querytest3|
|1970-01-01T08:00:00.003+08:00|  3querytest3 |         3querytest3|
+-----------------------------+--------------+--------------------+
```

## StrCmp

### Usage

The function is used to get the compare result of two input series. Returns `0` if series value are the same, a `negative integer` if value of series1 is smaller than series2, 
a `positive integer` if value of series1  is more than series2.

**Name:** StrCmp

**Input Series:** Support two input series. Data types are all the TEXT.

**Output Series:** Output a single series. The type is INT32.

**Note:** Returns NULL either series value is NULL.

### Examples

Input series:

```
+-----------------------------+--------------+--------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|
+-----------------------------+--------------+--------------+
|1970-01-01T08:00:00.001+08:00|        1test1|          null|
|1970-01-01T08:00:00.002+08:00|      22test22|      2222test|
+-----------------------------+--------------+--------------+
```

SQL for query:

```sql
select s1, s2, strcmp(s1, s2) from root.sg1.d1
```

Output series:

```
+-----------------------------+--------------+--------------+--------------------------------------+
|                         Time|root.sg1.d1.s1|root.sg1.d1.s2|strcmp(root.sg1.d1.s1, root.sg1.d1.s2)|
+-----------------------------+--------------+--------------+--------------------------------------+
|1970-01-01T08:00:00.001+08:00|        1test1|          null|                                  null|
|1970-01-01T08:00:00.002+08:00|      22test22|      2222test|                                    66|
+-----------------------------+--------------+--------------+--------------------------------------+
```


## StrReplace

### Usage

The function is used to replace the specific substring with given string.

**Name:** STRREPLACE

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `target`: The target substring to be replaced.
+ `replace`: The string to be put on.
+ `limit`: The number of matches to be replaced which should be an integer no less than -1,
  default to -1 which means all matches will be replaced.
+ `offset`: The number of matches to be skipped, which means the first `offset` matches will not be replaced, default to 0.
+ `reverse`: Whether to count all the matches reversely, default to 'false'.

**Output Series:** Output a single series. The type is TEXT.

### Examples

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2021-01-01T00:00:01.000+08:00|      A,B,A+,B-|
|2021-01-01T00:00:02.000+08:00|      A,A+,A,B+|
|2021-01-01T00:00:03.000+08:00|         B+,B,B|
|2021-01-01T00:00:04.000+08:00|      A+,A,A+,A|
|2021-01-01T00:00:05.000+08:00|       A,B-,B,B|
+-----------------------------+---------------+
```

SQL for query:

```sql
select strreplace(s1, "target"=",", "replace"="/", "limit"="2") from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------+
|                         Time|strreplace(root.test.d1.s1, "target"=",",|
|                             |              "replace"="/", "limit"="2")|
+-----------------------------+-----------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                A/B/A+,B-|
|2021-01-01T00:00:02.000+08:00|                                A/A+/A,B+|
|2021-01-01T00:00:03.000+08:00|                                   B+/B/B|
|2021-01-01T00:00:04.000+08:00|                                A+/A/A+,A|
|2021-01-01T00:00:05.000+08:00|                                 A/B-/B,B|
+-----------------------------+-----------------------------------------+
```

Another SQL for query:

```sql
select strreplace(s1, "target"=",", "replace"="/", "limit"="1", "offset"="1", "reverse"="true") from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------------------+
|                         Time|strreplace(root.test.d1.s1, "target"=",", "replace"= | 
|                             |    "|", "limit"="1", "offset"="1", "reverse"="true")|
+-----------------------------+-----------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                            A,B/A+,B-|
|2021-01-01T00:00:02.000+08:00|                                            A,A+/A,B+|
|2021-01-01T00:00:03.000+08:00|                                               B+/B,B|
|2021-01-01T00:00:04.000+08:00|                                            A+,A/A+,A|
|2021-01-01T00:00:05.000+08:00|                                             A,B-/B,B|
+-----------------------------+-----------------------------------------------------+
```

## RegexMatch

### Usage

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

### Examples

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

## RegexReplace

### Usage

The function is used to replace the specific regular expression matches with given string.

**Name:** REGEXREPLACE

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `regex`: The target regular expression to be replaced. All grammars supported by Java are acceptable.
+ `replace`: The string to be put on and back reference notes in Java is also supported,
  for example, '$1' refers to group 1 in the `regex` which will be filled with corresponding matched results.
+ `limit`: The number of matches to be replaced which should be an integer no less than -1,
  default to -1 which means all matches will be replaced.
+ `offset`: The number of matches to be skipped, which means the first `offset` matches will not be replaced, default to 0.
+ `reverse`: Whether to count all the matches reversely, default to 'false'.

**Output Series:** Output a single series. The type is TEXT.

### Examples

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
select regexreplace(s1, "regex"="192\.168\.0\.(\d+)", "replace"="cluster-$1", "limit"="1") from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------------------------+
|                         Time|regexreplace(root.test.d1.s1, "regex"="192\.168\.0\.(\d+)",|
|                             |                       "replace"="cluster-$1", "limit"="1")|
+-----------------------------+-----------------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                      [cluster-1] [SUCCESS]|
|2021-01-01T00:00:02.000+08:00|                                     [cluster-24] [SUCCESS]|
|2021-01-01T00:00:03.000+08:00|                                         [cluster-2] [FAIL]|
|2021-01-01T00:00:04.000+08:00|                                      [cluster-5] [SUCCESS]|
|2021-01-01T00:00:05.000+08:00|                                    [cluster-124] [SUCCESS]|
+-----------------------------+-----------------------------------------------------------+
```

## RegexSplit

### Usage

The function is used to split text with given regular expression and return specific element.

**Name:** REGEXSPLIT

**Input Series:** Only support a single input series. The data type is TEXT.

**Parameter:**

+ `regex`: The regular expression used to split the text.
  All grammars supported by Java are acceptable, for example, `['"]` is expected to match `'` and `"`.
+ `index`: The wanted index of elements in the split result.
  It should be an integer no less than -1, default to -1 which means the length of the result array is returned
  and any non-negative integer is used to fetch the text of the specific index starting from 0.

**Output Series:** Output a single series. The type is INT32 when `index` is -1 and TEXT when it's an valid index.

**Note:** When `index` is out of the range of the result array, for example `0,1,2` split with `,` and `index` is set to 3,
no result are returned for that record.

### Examples

Input series:

```
+-----------------------------+---------------+
|                         Time|root.test.d1.s1|
+-----------------------------+---------------+
|2021-01-01T00:00:01.000+08:00|      A,B,A+,B-|
|2021-01-01T00:00:02.000+08:00|      A,A+,A,B+|
|2021-01-01T00:00:03.000+08:00|         B+,B,B|
|2021-01-01T00:00:04.000+08:00|      A+,A,A+,A|
|2021-01-01T00:00:05.000+08:00|       A,B-,B,B|
+-----------------------------+---------------+
```

SQL for query:

```sql
select regexsplit(s1, "regex"=",", "index"="-1") from root.test.d1
```

Output series:

```
+-----------------------------+------------------------------------------------------+
|                         Time|regexsplit(root.test.d1.s1, "regex"=",", "index"="-1")|
+-----------------------------+------------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                                     4|
|2021-01-01T00:00:02.000+08:00|                                                     4|
|2021-01-01T00:00:03.000+08:00|                                                     3|
|2021-01-01T00:00:04.000+08:00|                                                     4|
|2021-01-01T00:00:05.000+08:00|                                                     4|
+-----------------------------+------------------------------------------------------+
```

Another SQL for query:

SQL for query:

```sql
select regexsplit(s1, "regex"=",", "index"="3") from root.test.d1
```

Output series:

```
+-----------------------------+-----------------------------------------------------+
|                         Time|regexsplit(root.test.d1.s1, "regex"=",", "index"="3")|
+-----------------------------+-----------------------------------------------------+
|2021-01-01T00:00:01.000+08:00|                                                   B-|
|2021-01-01T00:00:02.000+08:00|                                                   B+|
|2021-01-01T00:00:04.000+08:00|                                                    A|
|2021-01-01T00:00:05.000+08:00|                                                    B|
+-----------------------------+-----------------------------------------------------+
```