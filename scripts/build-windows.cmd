@echo off
setlocal
cd /d "%~dp0.."
cargo build --release --bin duta-stratumd
if errorlevel 1 exit /b 1
echo.
echo Release binary is in target\release\duta-stratumd.exe
