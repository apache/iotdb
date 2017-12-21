@echo off
echo ````````````````````````
echo Starting IoTDB Client
echo ````````````````````````

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%CD%
popd

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iotdb.client.Client
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -DIOTDB_HOME=%IOTDB_HOME%

REM For each jar in the IOTDB_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%IOTDB_HOME%\lib\*.jar") do call :append "%%i"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %*

goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL