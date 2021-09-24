

## TsFile Settle Tool

### Introduction

1. The settle tool helps you sort sealed TsFiles and mods files, filter out deleted data and generate new tsfiles.  If all the data in a TsFile is deleted, then the local tsFile and its corresponding resource file will be deleted.  
2. The TsFile settle tool can be used only for IOTDB v0.12, that is, it can only be used for TsFile which version is 3. If the version is later than this version, you must upgrade TsFile to V3 using the online upgrade tool first . 
3. TsFile settle tool can be divided into online settle tool and offline settle tool. Both of them record a settle log ("data\system\settle\settle.txt") during the settling process, which can be used to recover the failed files next time.  If there are any failed files in the log, they will be settled first.  

### Offline Settle Tool

The tool's startup scripts "settle.bat" and "settle.sh" are generated in the "server\target\iotdb-server-{version}\tools\tsfileToolSet" directory after server is compiled.  Run the script with at least one parameter, which can be a directory path or a specific TsFile path, separated by Spaces.  If the parameter is a directory, the offline settle tool will recursively search for all sealed TsFiles in the directory for settling.  If there are multiple parameters, the offline settle tool will find all TsFiles under the specified parameters and settle them one by one.  

When using the offline settle tool, ensure that the IOTDB server stops running; otherwise, an error may occur.  

#### 1. Syntax

```bash
#Windows
.\settle.bat <dirpath/TsFilePath> <dirpath/TsFilePath> ...

#MacOs or Linux
./settle.sh <dirpath/TsFilePath> <dirpath/TsFilePath> ...
```

#### 2. Example

```
>.\settle.bat C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0 

​````````````````````````
Starting Settling the TsFile
​````````````````````````
Totally find 3 tsFiles to be settled, including 0 tsFiles to be recovered.
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsfile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsfile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsfile
Finish settling all tsFiles successfully!
```

### Online Settle Tool

When the user uses the settle command on the IOTDB client, the online settle tool registers a settle service in the background, the Settle Service, which finds all TsFiles under the specified storage group and starts a settle thread for each TsFile.  The online settle tool does not allow users to delete the data in the virtual storage group until the settle is complete.  

#### 1. Syntax

```
IoTDB> Settle <StorageGroupId>
```

#### 2. Example

```
IoTDB> Settle root.ln;
Msg: The statement is executed successfully.
```