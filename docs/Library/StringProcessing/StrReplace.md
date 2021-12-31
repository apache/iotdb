# StrReplace

## Usage

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

## Examples

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
