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

# TsFile Reading Process

This chapter introduces how to read TsFile. The content is mainly divided into two parts, the introduction of Filters and Expressions , and the detailed illustration of query process in TsFile.

- [TsFile Read Process](#TsFile-Read-Process)
  - [1 Filters and Expressions](#1-Filters-and-Expressions)
    - [1.1 Filter](#11-Filter)
    - [1.2 Expression](#12-Expression)
      - [1.2.1 SingleSeriesExpression](#121-SingleSeriesExpression)
      - [1.2.2 GlobalTimeExpression](#122-GlobalTimeExpression)
      - [1.2.3 IExpression](#123-IExpression)
      - [1.2.4 Executable Expression](#124-Executable-Expression)
      - [1.2.5 The Algorithm Transforming IExpression to an Executable Expression](#125-The-Algorithm-Transforming-IExpression-to-an-Executable-Expression)
  - [2 Query Execution of TsFile](#2-Query-Execution-of-TsFile)
    - [2.1 Design](#21-Design)
    - [2.2 Three Components](#22-Three-Components)
      - [2.2.1 FileSeriesReader](#221-FileSeriesReader)
      - [2.2.2 FileSeriesReaderByTimestamp](#222-FileSeriesReaderByTimestamp)
      - [2.2.3 TimeGeneratorImpl](#223-TimeGeneratorImpl)
    - [2.3 Merge Query](#23-Merge-Query)
    - [2.4 Join Query](#24-Join-Query)
    - [2.5 Query of TsFile](#25-Query-of-TsFile)
    - [2.6 Related Concepts](#26-Related-Concepts)
## 1 Filters conditions and query experssions
### 1.1 Filter
This chapter introduces the filtering conditions and the relevant defination of query expression when TSFILE file is read first. Secondly, how to transform the filtering conditions entered by users into query conditions that the system can execute.

Filter represents the basic filtering condition. The user can specify specific filtering criteria on a timestamp or on a column of values. After distinguishing the filtering conditions of timestamp and column value, let t represent a certain timestamp constant. Filter has the following 12 basic types, which are inheritance relations in implementation.

Filter|Filter Type|Explanation|Examples
----|----|---|------
TimeEq|time filter|timestamp = some value|TimeEq(t) means timestamp should be equal to t 
TimeGt|time filter|timestamp > some value|TimeGt(t) means timestamp should be greater than t
TimeGtEq|time filter|timestamp >= some value|TimeGtEq(t) means timestamp greater than or equal to t
TimeLt|time filter|timestamp < some value|TimeLt(t) means timestamp should be less than t
TimeLtEq|time filter|timestamp <= some value|TimeLtEq(t) means timestamp should be less than or equal to t
TimeNotEq|time filter|timestamp != some value|TimeNotEq(t) means timestamp should not be equal to t
ValueEq|value filter|value on this column = some value|ValueEq(2147483649) means value on this column should be equal to 2147483649
ValueGt|value filter|value on this column > some value|ValueGt(100.5) means value on this column should be greater than 100.5
ValueGtEq|value filter|value on this column >= some value|ValueGtEq(2) means value on this column should be greanter than 2
ValueLt|value filter|value on this column < some value|ValueLt("string") means value on this column should be less than "string" in alphabet order
ValueLtEq|value filter|value on this column <= some value|ValueLtEq(-100) means value on this column shold be less than or equal to -100
ValueNotEq|value filter|value on this column != some value|ValueNotEq(true) means value on this column should not be true

A filter can consist of one or two sub-filters.。If a Filter is composed of a single Filter, it is also termed as UnaryFilter.If it contains two filters, it is termed as BinaryFilter. In this case, the two filters are connected with a logical relation, AND or OR, where the formar is termed as AndFilter, and the latter is OrFilter. Obviously, both AndFilter and OrFilter are BinaryFilter.

We give some examples of AndFilter and OrFilter, where "&&" indicates relation AND and "||" indicates relation OR.

1. AndFilter(TimeGt(100), TimeLt(200)) means "timestamp > 100 && timestamp < 200"
2. AndFilter (TimeGt(100), ValueGt(0.5)) means "timestamp > 100 && value > 0.5"
3. AndFilter (AndFilter (TimeGt(100), TimeLt(200)), ValueGt(0.5)) means "(timestamp > 100 && timestamp < 200) && value > 0.5"
4. OrFilter(TimeGt(100), ValueGt(0.5)) means "timestamp > 100 || value > 0.5"
5. OrFilter (AndFilter(TimeGt(100), TimeLt(200)), ValueGt(0.5)) means "(timestamp > 100 && timestamp < 200) || value > 0.5"

The formal definition of "Filter", "AndFilter" and "OrFilter" are shown below: 

    Filter := Basic Filter | AndFilter | OrFilter
    AndFilter := Filter && Filter
    OrFilter := Filter && Filter

For simplicity, we symbolize Basic Filter, AndFilter and OrFilter. Note that t is a variable of type INT64, v is a variable whose type can be BOOLEAN, INT32, INT64, FLOAT, DOUBLE or BINARY。

<style> table th:nth-of-type(2) { width: 150px; } </style>
Name|symbol|Examples
----|------------|------
TimeEq| time == t| time == 14152176545 means timestamp should be equal to 14152176545 
TimeGt| time > t| time > 14152176545 means timestamp should be greater than 14152176545
TimeGtEq| time >= t| time >= 14152176545 means timestamp should be greater than or equal to 14152176545
TimeLt| time < t| time < 14152176545 means timestamp should be less than 14152176545
TimeLtEq| time <= t| time <= 14152176545 means timestamp should be less than or equal to 14152176545
TimeNotEq| time != t| time != 14152176545 means timestamp  
should be equal to 14152176545
ValueEq| value == v| value == 10 means value should be equal to 10
ValueGt| value > v| value > 100.5 means value should be greater than 100.5
ValueGtEq| value >= v| value >= 2 means value should be greater than or equal to 2
ValueLt| value < v| value < "string" means value should be less than "string" in alphabet order
ValueLtEq| value <= v| value <= -100 means value should be less than or equal to -100
ValueNotEq| value != v| value != true means value should not be true
AndFilter| \<Filter> && \<Filter>| 1. value > 100 && value < 200 means value should be greanter than 100 and less than 200; <br>2. (value >= 100 && value <= 200) && time > 14152176545 means "value should be greater than or equal to 100 and value should be less than or equal to" and "timestamp should be greater than 14152176545"
OrFilter| \<Filter> &#124;&#124; \<Filter>| 1. value > 100 &#124;&#124; time >  14152176545, means value should be greater than 100 or timestamp should be greater than 14152176545;<br>2. (value > 100 && value < 200)&#124;&#124; time > 14152176545, means "value should be greater than 100 and value should be less than 200" or "timestamp should be greater than 14152176545"

### 1.2 Expression

When we assign a particular time series (including the timestamp) to a Filter, it becomes an expression. For example, "value > 10" can only describe a Filter, without the taste of a query. However, "the value of time series 'd1.s1' should be greater than 10" is an expression. Specifically, if the Filter only works on timestamp, it can be seen as an expression, which is called GlobalTimeExpression. The following sections introduces Expression in detail,

#### 1.2.1 SingleSeriesExpression

SingleSeriesExpression is an expression on a time series (excluding the timestamp column) with a Filter. A SingleSeriesExpression contains a Path and a Filter. The Path is the path of the time series, and the Filter indicate the filter condition, as is introduced in section 1.1.

The structure of SingleSeriesExpression is shown below: 

    SingleSeriesExpression
        Path: the path of the time series in the given SingleSeriesExpression
        Filter: filter condition

When querying, a SingleSeriesExpression claims that the data point in the time series should satisfy the constraints in the Filter.

Examples of SingleSeriesExpression are shown below.

Example 1. 

    SingleSeriesExpression
        Path: "d1.s1"
        Filter: AndFilter(ValueGt(100), ValueLt(200))

The SingleSeriesExpression claims that time series "d1.s1" should satisfy that "value should be greater than 100 and less than 200"。

We formalize the rule as SingleSeriesExpression("d1.s1", value > 100 && value < 200)

---------------------------
Example 2. 
    
    SingleSeriesExpression
        Path: "d1.s1"
        Filter: AndFilter(AndFilter(ValueGt(100), ValueLt(200)), TimeGt(14152176545))
    
The SingleSeriesExpression claims that time series "d1.s1" should satisfy that "value should be greater than 100 and less than 200 and timestamp should be greater than 14152176545"。
    
We formalize the rule as SingleSeriesExpression("d1.s1", (value > 100 && value < 200) && time > 14152176545)

#### 1.2.2 GlobalTimeExpression
GlobalTimeExpression means a global time filter. A GlobalTimeExpression contains a Filter, which is only composed of time filters (value filters not allowed). When querying, a GlobalTimeExpression claims that data points of all selected time series should satisfy the constraints in the Filter. The structure of GlobalTimeExpression is shown below: 

    GlobalTimeExpression
        Filter: a filter which only contains time filters。
        The formalized definition of Filter here is: 
            Filter := TimeFilter | AndExpression | OrExpression
            AndExpression := Filter && Filter
            OrExpression := Filter && Filter

Some formalized examples of GlobalTimeExpression are shown below:
1. GlobalTimeExpression(time > 14152176545 && time < 14152176645) claims that the all selected time series should satisfy that the timestamp should be "greater than 14152176545 and less than 14152176645"
2. GlobalTimeExpression((time > 100 && time < 200) || (time > 400 && time < 500)) claims that all selected time series should satisfy that the timestamp should be "greater than 100 and less than 200" or "greater than 400 and less than 500"

#### 1.2.3 IExpression
IExpression indicates the all filters with the corresponding columns in a query. 
An IExpression can be a SingleSeriesExpression or a GlobalTimeExpression. In this case, the IExpression is a UnaryExpression. An IExpression can also contains two IExpressions, connected with relation AND or OR. Two IExpressions joined with AND relation is termed as AndExpression. Likewise, two IExpressions joined with OR relation is termed as OrExpression. An IExpression containing two children is termed BinaryExpression. UnaryExpression and BinaryExpression are both IExpression.

The formalized definition of IExpression is shown below:

    IExpression := SingleSeriesExpression | GlobalTimeExpression | AndExpression | OrExpression
    AndExpression := IExpression && IExpression
    OrExpression := IExpression || IExpression

We use a tree-like structure to formalize an IExpression. Here are some examples.

1. An IExpression only contains a SingleSeriesExpression: 
   
        IExpression(SingleSeriesExpression("d1.s1", value > 100 && value < 200))

2. An IExpression only contains a GlobalTimeExpression: 

        IExpression(GlobalTimeExpression (time > 14152176545 && time < 14152176645))

3. An IExpression contains multiple SingleSeriesExpressions: 

        IExpression(
            AndExpression
                SingleSeriesExpression("d1.s1", (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression("d1.s2", value > 0.5 && value < 1.5)
        )

    **Note**: The IExpression is an AndExpression, where the time series "d1.s1" and "d1.s2" should satisfy the constraints in the Filter.

4. An IExpression contains both SingleSeriesExpressions and GlobalTimeExpressions

        IExpression(
            AndExpression
                AndExpression
                    SingleSeriesExpression("d1.s1", (value > 100 && value < 200) || time > 14152176645)
                    SingleSeriesExpression("d1.s2", value > 0.5 && value < 1.5)
                GlobalTimeExpression(time > 14152176545 && time < 14152176645)
        )

    **Note**: The IExpression is an AndExpression, where the time series "d1.s1" and "d1.s2" should not only satisfy the constraints in corresponding Filters in SingleSeriesExpression, but also the constraints of the GlobalTimeExpression.

#### 1.2.4 Executable Expression

To make the query execution more comprehensible, we give the concept of executable expression. An executable expression is a particular kinds of IExpression. The IExpression customized by system user can be transformed to an executable expression by some algorithm, which will be introduced in the following sections. An executable expression is an IExpression satisfying one of the following constraints:
1. The IExpression is a single GlobalTimeExpression
2. The IExpression is a single SingleSeriesExpression
3. The IExpression is an AndExpression, whose leaf nodes are SingleSeriesExpressions
4. The IExpression is an OrExpression, whose leaf nodes are SingleSeriesExpressions

The formalized definition of an executable expression is shown below:

    executable expression := SingleSeriesExpression| GlobalTimeExpression | AndExpression | OrExpression
    AndExpression := < ExpressionUNIT > && < ExpressionUNIT >
    OrExpression := < ExpressionUNIT > || < ExpressionUNIT >
    ExpressionUNIT := SingleSeriesExpression | AndExpression | OrExpression

Some examples of executable expression and non-executable expression are show below:

Example 1: 

    IExpression(SingleSeriesExpression("d1.s1", value > 100 && value < 200))

Is an executable expression? Yes

**Hint**: The IExpression is a single SingleSeriesExpression, satisfying constraint 1

----------------------------------
Example 2: 

    IExpression(GlobalTimeExpression (time > 14152176545 && time < 14152176645))

Is an executable expression? Yes

**Hint**: The IExpression is a single GlobalTimeExpression, satisfying constraint 2

-----------------------
Example 3: 

    IExpression(
        AndExpression
            GlobalTimeExpression (time > 14152176545)
            GlobalTimeExpression (time < 14152176645)
    )

Is an executable expression? No

**Hint**: The IExpression is an AndExpression, but it contains GlobalTimeExpressions, which is against constraint 3

--------------------------

Example 4: 

    IExpression(
        OrExpression
            AndExpression
                SingleSeriesExpression("d1.s1", (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression("d1.s2", value > 0.5 && value < 1.5)
        SingleSeriesExpression("d1.s3", value > "test" && value < "test100")
    )

Is an executable expression? Yes

**Hint**: The IExpression is an OrExpression, whose leaf nodes are all SingleSeriesExpressions, which satisfies constraints 4.

----------------------------

Example 5: 

    IExpression(
        AndExpression        
            AndExpression
                SingleSeriesExpression("d1.s1", (value > 100 && value < 200) || time > 14152176645)
                SingleSeriesExpression("d1.s2", value > 0.5 && value < 1.5)
            GlobalTimeExpression(time > 14152176545 && time < 14152176645)
    )

Is an executable expression? No

**Hint**: The IExpression is an AndExpression, but one of the leaf nodes is a GlobalTimeExpression, which is against constraint 3.

#### 1.2.5 The Algorithm Transforming IExpression to an Executable Expression

In this section, we introduce how to transform an IExpression to be executable.

If an IExpression is not executable, it is either an AndExpression, or an OrExpression. Moreover, it contains both GlobalTimeExpressions and SingleSeriesExpressions. According to the definition in the preceding sections, an AndExpression or OrExpression is composed of two IExpressions, that 

    AndExpression := <IExpression> AND <IExpression>
    OrExpression := <IExpression> OR <IExpression>

We denote the left child and right child as LeftIExpression and RightIExpression, so that

    AndExpression := <LeftIExpression> AND <RightIExpression>
    OrExpression := <LeftIExpression> OR <RightIExpression>

The declaration of the method is shown below.

    IExpression optimize (IExpression expression, List<Path> selectedSeries)

    Input: The IExpression to be transformed, and the projected time series
    Output: The transformed IExpression, which is executable

Before the introducing the optimize() method in detail, we first show how to combine two expressions or filters, which is very useful in the optimize() method.

* combineTwoGlobalTimeExpression combines two GlobalTimeExpressions to be a single GlobalTimeExpression。
  
  This method receives three inputs. The declaration of the method is: 

        GlobalTimeExpression combineTwoGlobalTimeExpression(
            GlobalTimeExpression leftGlobalTimeExpression,
            GlobalTimeExpression rightGlobalTimeExpression,
            ExpressionType type)

        Input 1: leftGlobalTimeExpression
        Input 2: rightGlobalTimeExpression
        Input 3: type, the relation of the two expressions, which is either "AND" or "OR"

        Output: GlobalTimeExpression, which is the merged expression
    
    The method contains two steps: 
    1. Denote the Filter in leftGlobalTimeExpression as filter1, in rightGlobalTimeExpression as filter2. It first merges filter1 and filter2 to be a new Filter, denoted as filter3. The method of merging two filters is shown in MergeFilter in the following section.
    2. Generate a new GlobalTimeExpression, and assign filter3 to be its Filter. This new GlobalTimeExpression is the result.

    An example of combining two GlobalTimeExpressions is shown below.

    The three inputs are: 

        leftGlobalTimeExpression: GlobalTimeExpression(Filter: time > 100 && time < 200)
        rightGlobalTimeExpression: GlobalTimeExpression(Filter: time > 300 && time < 400)
        type: OR

    than, the merged expression is 

        GlobalTimeExpression(Filter: (time > 100 && time < 200) || (time > 300 && time < 400))

* MergeFilter merges two Filters. This method receives three inputs: 

        Filter1: The first Filter to merge
        Filter2: The second Filter to merge
        Relation: The relation between the two Filters (either AND or OR)

    than, the execution strategy of this method is

        if relation == AND:
            return AndFilter(Filter1, Filter2)
        else if relation == OR:
            return OrFilter(Filter1, Filter2)

    This method is implemented in `AndFilter and(Filter left, Filter right)` and `OrFilter or(Filter left, Filter right)` in class FilterFactory.

* handleOneGlobalExpression merges a GlobalTimeExpression and an IExpression to be one executable expression. The result of the method only contains SingleSeriesExpressions. The declaration of the function is shown below: 

        IExpression handleOneGlobalTimeExpression(
            GlobalTimeExpression globalTimeExpression,
            IExpression expression,
            List<Path> selectedSeries, 
            ExpressionType relation)

        Input 1: GlobalTimeExpression
        Input 2: IExpression
        Input 3: time series to project
        Input 4: the relation between the two expressions (either AND or OR)

        Output: an IExpression which is executable

    This method first calls optimize() to transform the second input IExpression to be executable, which is recursive in view of optimize(), and then combine them in two ways.

    *Case 1*: The relation between GlobalTimeExpression and the optimized IExpression is AND. In this case, denote the Filter in GlobalTimeExpression as Filter. We just need to merge the tFilter to the Filters of each SingleSeriesExpression in the IExpression。This method is implemented in `void addTimeFilterToQueryFilter(Filter timeFilter, IExpression expression)`. Here is an example: 

    To combine the following GlobaLTimeFilter and IExpression where

        1. GlobaLTimeFilter(tFilter)
        2. IExpression
                AndExpression
                    OrExpression
                        SingleSeriesExpression("path1", filter1)
                        SingleSeriesExpression("path2", filter2)
                    SingleSeriesExpression("path3", filter3)

    The result is

        IExpression
            AndExpression
                OrExpression
                    SingleSeriesExpression("path1", AndFilter(filter1, tFilter))
                    SingleSeriesExpression("path2", AndFilter(filter2, tFilter))
                SingleSeriesExpression("path3", AndFilter(filter3, tFilter))

    *Case 2*: The relation between GlobalTimeExpression and IExpression is OR. In this case, the merge steps include: 
    1. Analyse the projected time series, which is a set of Path. To take a query with 3 projected time series as an example, denote that projected time series as a set, PathList{path1, path2, path3}。
    2. Denote the Filter in GlobalTimeExpression to be tFilter. The method calls pushGlobalTimeFilterToAllSeries() to generate a corresponding SingleSeriesExpression for each Path. Set the Filters of SingleSeriesExpressions to be tFilter. Join the generated SingleSeriesExpression with OR operator to get an OrExpression, which is denoted as orExpression.
    3. Call mergeSecondTreeToFirstTree method to combine the nodes in IExpression with the nodes in the orExpression, which is generated from step 2. The combined structure is the result expression.
    
    For example, to combine the following GlobalTimeFilter and IExpression using OR relation, denote the projected time series as PathList{path1, path2, path3}

        1. GlobalTimeFilter(tFilter)
        2. IExpression
                AndExpression
                    SingleSeriesExpression("path1", filter1)
                    SingleSeriesExpression("path2", filter2)

    the result is

        IExpression
            OrExpression
                AndExpression
                    SingleSeriesExpression("path1", filter1)
                    SingleSeriesExpression("path2", filter2)
                OrExpression
                    OrExpression
                        SingleSeriesExpression("path1", tFilter)
                        SingleSeriesExpression("path2", tFilter)
                    SingleSeriesExpression("path3", tFilter)

* MergeIExpression combines two IExpressions to be one executable expression. This method receives three inputs, respectively

        IExpression1: the first IExpression to merge
        IExpression2: the second IExpression to merge
        relation: the relation between the two IExpressions (AND or OR)

    The strategy of the method is: 

        if relation == AND:
            return AndExpression(IExpression1, IExpression2)
        else if relation == OR:
            return OrExpression(IExpression1, IExpression2)

Using the above four combination methods, the steps of optimize() include:
1. If the IExpression is a UnaryExpression, (a single SingleSeriesExpression or GlobalTimeExpression), return the IExpression without any operations. Otherwise, go to step 2.
2. When it reaches this step, it means that the IExpression is an AndExpression or OrExpression.
   
   a. If the LeftIExpression and RightIExpression both are GlobalTimeExpression, use combineTwoGlobalTimeExpression method, and output the result.

   b. If LeftIExpression is a GlobalTimeExpression, and RightIExpression is not a GlobalTimeExpression, call handleOneGlobalExpression() to combine them。

   c. If LeftIExpression is not a GlobalTimeExpression, while RightIExpression is, call handleOneGlobalExpression() to combine them.

   d. If neither LeftIExpression nor RightIExpression is a GlobalTimeExpression, recursively call optimize() method on both LeftIExpression  and RightIExpression to get the left executable expression and the right executable expression. Then use mergeIExpression method to generate a final IExpression which is executable.

## 2 Query Execution of TsFile

### 2.1 Design

The interface of TsFile on file level is intended only for queries on original data. According to the existence of value filter, the query can be divided into two groups: those without filters or only with a time filter, and those containing value filters.

To execute the two kinds of queries, we design two query execution methods:

* merge query

    It generates multiple readers, aligns the result in time, and returns the result set.

* join query

    According to the query conditions, it generates satisfying timestamp, which is used for generating result set.

### 2.2 Three Components 

#### 2.2.1 FileSeriesReader
org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader

**Functions**: FileSeriesReader queries the data points of a time series in a file, which satisfies the filter conditions. It outputs the data points of the given time series in the given file in timestamp ascending order. There can be no filters at all.

**Implementation**: FileSeriesReader retrieves the Chunk information of the given time series according to the Path, it then traverses each Chunk in timestamp ascending order, and outputs the satisfying data points. 

#### 2.2.2 FileSeriesReaderByTimestamp

org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp

**Functions**: FileSeriesReaderByTimestamp queries the data points of a time series in a file, whose timestamp satisfies the timestamp constraints.

**Implementation**: This component provides an interface, getValueInTimestamp(long timestamp), which receives increasing timestamp value,  and outputs the data points on the time series whose timestamp is identical. If there's no such data point, the result is null.

#### 2.2.3 TsFileTimeGenerator
org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator

**Functions**: According to the filter condition, TimeGeneratorImpl generates the satisfying timestamp. It first transforms the filter conditions to be a binary tree, and recursively generate the satisfying timestamp. This component is used in executing join query.

An executable expression contains one or nultiple SingleSeriesExpressions. The relation between two SingleSeriesExpressions is either AND or OR. Therefore, the filter in an executable expression can be transformed to a binary tree, where the leaf nodes are FileSeriesReader, and the non-leaf nodes are AndNode or OrNode. Particularly, when the expression only contains a single SingleSeriesExpression, the binary tree only has one node. The satisfying timestamp can be generated using the binary tree. 

This component provides two basic functions: 

1. check if there exists a next satisfying timestamp

2. get the next satisfying timestamp

### 2.3 Query Merging
org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator

Suppose there are n time series. For each one of them, a FileSeriesReader will be generated. If there's a GlobalTimeExpression, the filter in it will be input to the FileSeriesReaders.

A DataSetWithoutTimeGenerator will be generated using these FileSeriesReaders. Since each FileSeriesReader outputs data points in time stamp ascending order, we can use the idea of k-way merge to align the result data points. 

The steps include: 

(1) Create a min-heap, which stores timestamps. The min-heap is organized according to the value of the timestamp.

(2) Initialize the min-heap. Access each FileSeriesReader once. If there's a next data point in the FileSeriesReader, add its timestamp to the min-heap. Till now, the heap contains at most one timestamp of each time series, which is the minimum in its corresponding time series.

(3) If the size of the heap is greater than 0, take the timestamp on the top of the heap, denote it as t, and remove it from the heap, then go to step (4). If the number of the heap is 0, go to step (5).

(4) Create a new RowRecord. In turns access each time series. When dealing with a time series, first check if there's a next data point, if it fails, set the result data point to be null. Otherwise, check if minimum timestamp in the time series is identical to t. If it is not, set the result data point to be null. Otherwise, get the data point and set it to be the result data point. If there exists a next data point after that. If it has, then set timestamp of the next data point in the time series to be its minimum timestamp. After accessing all time series, combine the result data points to form a RowRecord. Finally, go to step (3).

(5) Terminate the process.

### 2.4 Join Queries

org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator

Join query execution generates timestamp according to the filter conditions, and retrieves the data point on the projected time series to form a RowRecord. Main steps include;

(1) According to QueryExpression, initialize the timestamp generation module, TimeGeneratorImpl

(2) Generate FileSeriesReaderByTimestamp for each projected time series.

(3) If there exists a next timestamp in the "timestamp generation module", denote the timestamp t, go to step (4).Otherwise, terminate the query execution.

(4) According to t, utilize FileSeriesReaderByTimestamp on each time series to retrieve the data point whose corresponding timestamp is t. If there's no such data point, use null to represent it.

(5) Combine the data points in step (4) to form a RowRecord, then go to step (3) to generate another row of records.

### 2.5 Query entrence of TsFile

 org.apache.iotdb.tsfile.read.query.executor.TsFileExecutor

TsFileExecutor receives a QueryExpression, execute the query and outputs the QueryDataSet。The work flow includes the following steps: 

(1) Receive a QueryExpression.

(2) If the QueryExpression contains no filter conditions, execute merge query. If it contains any Filters, use ExpressionOptimizer to optimize the IExpression in QueryExpression. If the optimized IExpression is a GlobalTimeExpression, execute merge query. If it contains value filters, it sends a message to ExecutorWithTimeGenerator to execute join query.

(3) Generate the QueryDataSet. It iteratively generates RowRecord and returns the results.


### 2.6 Related Concepts

* Chunk: Chunk is the storage structure of a chunk of time series. IChunkReader is for reading the content。

* ChunkMetaData: ChunkMetaData records the offset, data type and encoding info of the Chunk in the File. 
  
* IMetadataQuerier: IMetadataQuerier is a querier for TsFile metadata. It queries the metadata of a file, and the ChunkMetaData of a time series.

* IChunkLoader:  IChunkLoader is the loader for Chunk, whose main function is getting the corresponding Chunk of the given the ChunkMetaData.

* IChunkReader: IChunkReader reads the data in a Chunk. It receives a Chunk and parse it according to the info in the ChunkHeader. It provides two interface: 

    * hasNextSatisfiedPage & nextPageData: iteratively returns a Page
    * getPageReaderList: return all PageReader

* IPageReader: IPageReader reads the data in a Page. It provides two interface:

    * getAllSatisfiedPageData(): return all satisfying values
    * getStatistics(): return the statistic information of the Page

* QueryExpression

    QueryExpression is the query expression, which contains the time series to project, and filter constraints.

* QueryDataSet

    The result of a query. It contains one or multiple RowRecord, which combines data points having identical time stamp together. QueryDataSet provides two interface: 

    * check if there's a next RowRecord.
    * return the next RowRecord.

