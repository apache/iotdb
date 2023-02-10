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
     	  <version>1.0.0</version>
    </dependency>
</dependencies>
```


## Run TsFileWrite.java


  This class is to show how to write a TsFile. It provided two ways:
  
   The first one is using a JSON string for measurement(s). The JSON string is an array of JSON 
   objects(schema). The object must include the *measurement_id*, *datatype*, *encoding*, and 
   *compressor*. 
   
   An example JSON string is provided in the comments in 
   the method
   
        public static void tsFileWriteWithJson() throws IOException,WriteProcessException
   It uses this interface
   
        public void addMeasurementByJson(JSONObject measurement) throws WriteProcessException  
   An alternative way is to add these measurements directly(manually) by the second interface: 
   
         public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
   
   The method
   
         public static void tsFileWriteDirect() throws IOException,WriteProcessException
   shows how to use that interface.
   
   Note that the measurements in the two methods are the same therefore there output TsFile should also be identical.


  
## Run TsFileRead.java

 This class is to show how to read TsFile file named "testDirect.tsfile".
 
 The TsFile file "testDirect.tsfile" is generated from class TsFileWrite.
 
 It generates the same TsFile(testDirect.tsfile and testWithJson.tsfile) file by two different ways
 
 Run TsFileWrite to generate the testDirect.tsfile first
 
## Run TsFileSequenceRead.java

  This class is to show the structure of a TsFile.

### Notice 
  For detail, please refer to https://github.com/apache/iotdb/blob/master/tsfile/README.md.
