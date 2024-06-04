@echo off
setlocal enabledelayedexpansion

set "template_file=conf\iotdb-system.properties.template"
set "output_file=conf\iotdb-system.properties"
ren "%output_file%" "%template_file%"

(for /f "usebackq delims=" %%A in ("%template_file%") do (
    set "line=%%A"
    set "line=!line:~0,1!"
    if not "!line!"=="#" if not "!line!"=="" echo %%A
)) > "%output_file%"
