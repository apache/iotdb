# Artifact Guideline

## Minimum Artifact

To simplify the reproducing, the minimum artifact bypasses the IoTDB server, writes and reads the tsfile directly.

### Requirements

+ JDK >= 1.8
+ Maven >= 3.6


### Example Invocation


1. [Download](https://cloud.tsinghua.edu.cn/f/4602e518f32a4ceda877/) the datasets and put all CSV files into the directory `artifact/dataset`;

2. Install the dependencies into local repository: 

```sh
cd tsfile
mvn install -DskipTests
cd ..
```

3. Compile and package the artifact:

```sh
cd artifact
mvn compile assembly:single
```

4. Run the artifact and find the experimental results in the directory `artifact/result`:

```sh
java -jar target\artifact-0.14.0-SNAPSHOT-jar-with-dependencies.jar
```

## Key Source Codes

+ `library-udf\src\main\java\org\apache\iotdb\library\frequency\UDTFSTFT.java` is the source of Fourier transform and quantization (Section 2.2) 
+ `tsfile\src\main\java\org\apache\iotdb\tsfile\encoding\encoder\DescendEncoder.java` is the source of the encoder of descending bit-packing (Section 2.3-2.4)
+ `tsfile\src\main\java\org\apache\iotdb\tsfile\encoding\decoder\DescendDecoder.java` is the source of the decoder of descending bit-packing (Section 2.5-2.6)
+ `tsfile\src\main\java\org\apache\iotdb\tsfile\encoding\encoder\FreqEncoder.java` is the source of the encoder of time domain data encoding (Appendix D.1 in [full version](https://sxsong.github.io/doc/frequency.pdf))
+ `tsfile\src\main\java\org\apache\iotdb\tsfile\encoding\decoder\FreqDecoder.java` is the source of the decoder of time domain data encoding (Appendix D.1 in [full version](https://sxsong.github.io/doc/frequency.pdf))


## Usage in Apache IoTDB

Trying frequency domain data encoding in Apache IoTDB is a little more complex. 

1. Deploy Apache IoTDB (see [Quick Start](https://iotdb.apache.org/UserGuide/V0.13.x/QuickStart/QuickStart.html) in detail);
2. Register the UDF **STFT** (see [UDF](https://iotdb.apache.org/UserGuide/V0.13.x/Process-Data/UDF-User-Defined-Function.html) in detail);
3. Load the time series into IoTDB (see [CSV Tool](https://iotdb.apache.org/UserGuide/V0.12.x/System-Tools/CSV-Tool.html) in detail);
4. Write back the results of **STFT** to a time series (see Appendix B in [full version](https://sxsong.github.io/doc/frequency.pdf) in details).
