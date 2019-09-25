@echo off
set THRIFT_EXE=C:\bin\thrift-0.12.0.exe
set BAT_DIR=%~dp0
set THRIFT_SCRIPT=%BAT_DIR%..\service-rpc\src\main\thrift\rpc.thrift
set THRIFT_OUT=%BAT_DIR%target

rm -rf %THRIFT_OUT%
mkdir %THRIFT_OUT%
%THRIFT_EXE% -gen py -out %THRIFT_OUT%  %THRIFT_SCRIPT%
