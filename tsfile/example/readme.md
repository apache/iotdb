# Function
```
The example is to show how to write and read a TsFile File.
```
# Usage
## Dependencies with Maven

```
<dependencies>
    <dependency>
        <groupId>cn.edu.tsinghua</groupId>
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
            public void addMeasurement(MeasurementDescriptor measurementDescriptor) throws WriteProcessException
```

### Notice 
  Class TsFileWrite1 and class TsFileWrite2 are two ways to construct a TsFile instance,they generate the same TsFile file.
  
## Run TsFileRead.java

```
  The class is to show how to read TsFile file named "test.ts".
  The TsFile file "test.ts" is generated from class TsFileWrite1 or class TsFileWrite2, they generate the same TsFile file by two different ways
```

### Notice 
  For detail, please refer to https://github.com/thulab/tsfile/wiki/Get-Started.
