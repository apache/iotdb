

## TsFile整理工具

整理工具可以帮助你整理指定目录下所有已封口的TsFile文件和.mods文件，过滤掉删除的数据并生成新的TsFile。整理完后会将本地旧的.mods文件删除，若某TsFile里的数据被全部删除了，则会删除本地该TsFile文件和对应的.resource文件。TsFile整理工具分为在线整理工具和离线整理工具。



注意：该整理工具是线下的，在使用的时候必须保证IOTDB Server是停止运行的，否则会出现整理错误。另外，我们也有线上整理工具在后台自动帮助用户进行整理，详见设计文档。

### 使用

该工具的启动脚本settle.bat和settle.sh在编译了server后会生成至server\target\iotdb-server-{version}\tools\tsfileToolSet目录中。运行脚本时要给定一个参数，该参数是待整理TsFile所在的根目录，整理工具会去该目录下找到所有已封口的TsFile进行整理。

#### 1. 运行方法

```bash
#Windows
.\settle.bat <TsFile文件所在根目录路径>

#MacOs or Linux
./settle.sh <TsFile文件所在根目录路径>
```

#### 2. 运行示例

```
>.\settle.bat C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0

​````````````````````````
Starting Settling the TsFile
​````````````````````````
Totally find 3 tsFiles to be settled :
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631261328514-1-0-2.tsfile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631274465662-3-0-1.tsfile
Start settling for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsfile
Finish settling successfully for tsFile : C:\IOTDB\sourceCode\choubenson\iotdb\data\data\sequence\root.ln\0\0\1631433121335-5-0-0.tsfile
Settle Completely!
```



### 注意事项

1. 整理工具只能对版本为V3的TsFile进行整理，若低于此版本的应该先使用在线升级工具将TsFile升级到V3版本。
2. 目前仅支持输入的参数为TsFile所在的根目录，整理工具会去该目录下寻找所有待整理的已封口TsFile进行整理。
3. 整理工具在整理完每个TsFile后会删除该TsFile的mods文件。若某TsFile里的数据都被删掉了，则整理工具会直接删除该TsFile文件和对应的.resource文件。
4. 在整理过程中不允许用户对该虚拟存储组下的数据有任何的删除操作，直到整理完毕。