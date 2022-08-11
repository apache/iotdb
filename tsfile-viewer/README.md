
# tsfile-mt
[![Main Mac and Linux](https://github.com/apache/iotdb/actions/workflows/main-unix.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-unix.yml)
[![Main Win](https://github.com/apache/iotdb/actions/workflows/main-win.yml/badge.svg)](https://github.com/apache/iotdb/actions/workflows/main-win.yml)
# Outline
- [Introduction](#Introduction)
- [Quick Start](#quick-start)
    - [Prerequisites](#Prerequisites)
    - [Compile](#Compile)
- [Maintainers](#Maintainers)
- [Contributing](#Contributing)
- [Contributors](#Contributors)
# Introduction
tsfile-mt is a TsFile management tool. Currently, we support bit granularity parsing of TsFile and provide
visual display.
1. ChunkGroup and Index display: This tool can Clearly display information of each part of TsFile, details are as follows:
   1. The versionNumber.
   2. The data layer: contains details of each level and statistic information.
   
      i. ChunkGroup
   
      ii. Chunk
   
      iii. Page
   
      iv. Point
   3. The index layer: displayed in a B+ tree like structure then you can easily view the overall structure of the secondary 
   index(entity and measurement granularity).

2. Timeseries and measurement search: In addition to displaying data, we also provide the function of querying TimeSeries by keyword. There is a linkage
between the index layer and the data layer, it can quickly locate the desired TimeSeries with details.

3. The encoding and compression type of a timeseries analysis: tsfile-mt provide the analysis of the current timeseries encoding and compression. In addition, tsfile-mt also provide the analysis
   of the combination of various encoding and compression types of the timeseries.

# Quick Start
## Prerequisites
To use Tool-offline-for-IoTDB, you need to have:
1. Java >= 1.8 (Note: Because we use JavaFx to develop front end and JavaFx is dependent in JDK11, if the version of
   the JDK you use > 1.8, you need to import the relevant libraries.)
2. Maven >= 3.6
## Compile
You can download the source code from:
```
git clone https://github.com/lpf4254302/tsfile-mt.git
```
Under the root path of tsfile-mt:
```
mvn clean install
```
Under the root path of tsfile-mt-ui project, use the following command to assemble the project into a executable jar:
```
mvn assembly:assembly -D skipTests
```
The you can use the following command to start the project directly:
```
java -cp .\tsfile-mt-ui-0.2.0-SNAPSHOT-jar-with-dependencies.jar org.apache.iotdb.tool.ui.IUIApplication
```

# Maintainers
[@lpf4254302](https://github.com/RichardLitt)
# Contributing
Feel free to dive in! Open an issue or submit PRs.
# Contributors
This project exists thanks to all the people who contribute.
