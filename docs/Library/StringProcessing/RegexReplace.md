# RegexReplace

## Usage

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
