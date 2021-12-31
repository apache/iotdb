# RegexSplit

## Usage

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