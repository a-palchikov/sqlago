@echo off
if exist %~dp0test.db del /f %~dp0test.db
"%sqlany11%\bin64\dbinit" test -ze utf8 -zn utf8 -z uca -n
