@echo off

set "source_file=src\assembly\resources\conf\iotdb-system.properties"
set "target_template_file=target\conf\iotdb-system.properties.template"
set "target_properties_file=target\conf\iotdb-system.properties"

if exist "%target_template_file%" (
    del "%target_template_file%"
)

if exist "%target_properties_file%" (
    del "%target_properties_file%"
)

mkdir "%target_template_file%\.."

copy "%source_file%" "%target_template_file%"

for /f "tokens=* delims=" %%i in ('findstr /v "^[ ]*#" "%target_template_file%" ^| findstr /r /v "^$"') do (
    echo %%i >> "%target_properties_file%"
)
