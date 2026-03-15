@echo off
setlocal
cd /d "%~dp0.."
set "DAEMON=%~1"
if "%DAEMON%"=="" set "DAEMON=http://127.0.0.1:19085"
set "BIND=%~2"
if "%BIND%"=="" set "BIND=127.0.0.1:11001"
cargo run -- --bind "%BIND%" --daemon "%DAEMON%"
