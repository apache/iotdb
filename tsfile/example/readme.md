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

# Function
```
The example is to show how to write and read a TsFile File.
```
# Usage
## Dependencies with Maven

```
<dependencies>
    <dependency>
        <groupId>org.apache.iotdb</groupId>
        <artifactId>tsfile</artifactId>
     	  <version>0.7.0</version>
    </dependency>
</dependencies>
```

## Run TsFileWrite1.java

```
  The class is to show how to write Tsfile by using json schema,it use the first interface: 
            public TsFileWriter(File file) throws WriteProcessException, IOException
```

## Run TsFileWrite2.java

```
  The class is to show how to write Tsfile directly,it use the second interface: 
            public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
```

### Notice 
  Class TsFileWrite1 and class TsFileWrite2 are two ways to construct a TsFile instance,they generate the same TsFile file.
  
## Run TsFileRead.java

```
  The class is to show how to read TsFile file named "test.tsfile".
  The TsFile file "test.tsfile" is generated from class TsFileWrite1 or class TsFileWrite2, they generate the same TsFile file by two different ways
```

### Notice 
  For detail, please refer to https://github.com/thulab/tsfile/wiki/Get-Started.
