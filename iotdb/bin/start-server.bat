@echo off
echo ````````````````````````
echo Starting IoTDB
echo ````````````````````````

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..
if NOT DEFINED TSFILE_HOME set TSFILE_HOME=%CD%
popd

set TSFILE_CONF=%TSFILE_HOME%\conf
set TSFILE_LOGS=%TSFILE_HOME%\logs

IF EXIST %TSFILE_CONF%\iotdb-env.bat (
	CALL %TSFILE_CONF%\iotdb-env.bat
	) ELSE (
	echo "can't find %TSFILE_CONF%/iotdb-env.bat"
	)

if NOT DEFINED MAIN_CLASS set MAIN_CLASS=cn.edu.tsinghua.iotdb.service.Daemon
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
set CLASSPATH=%CLASSPATH%;iotdb.IoTDB
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1
goto :eof

REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp %CLASSPATH% %TSFILEDB_DERBY_OPTS% %TSFILEDB_JMX_OPTS% %MAIN_CLASS%
goto finally


:err
echo JAVA_HOME environment variable must be set!
pause


@REM -----------------------------------------------------------------------------
:finally

pause

ENDLOCAL