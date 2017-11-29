@echo off
echo ````````````````````````
echo Starting IoTDB Client
echo ````````````````````````

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED TSFILE_HOME set TSFILE_HOME=%CD%
popd

set TSFILE_CONF=%TSFILE_HOME%\conf
set TSFILE_LOGS=%TSFILE_HOME%\logs
if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iotdb.client.Client
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%TSFILE_CONF%\logback.xml"^
 -DTSFILE_HOME=%TSFILE_HOME%

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
set CLASSPATH="TSFILE_HOME%\conf"

REM For each jar in the TSFILE_HOME lib directory call append to build the CLASSPATH variable.
for %%i in ("%TSFILE_HOME%\lib\*.jar") do call :append "%%i"
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath

"%JAVA_HOME%\bin\java" -DTSFILE_HOME=%TSFILE_HOME% %JAVA_OPTS% -cp %CLASSPATH% %MAIN_CLASS% %*

goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

ENDLOCAL