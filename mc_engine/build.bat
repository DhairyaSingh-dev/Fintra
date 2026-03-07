@echo off
REM Build script for Monte Carlo WebAssembly module on Windows
REM Requires Emscripten SDK (emsdk) to be installed and activated

setlocal enabledelayedexpansion

set SCRIPT_DIR=%~dp0
set PROJECT_ROOT=%SCRIPT_DIR%..
set WASM_DIR=%PROJECT_ROOT%\static\wasm

echo ==========================================
echo Building Monte Carlo WASM Module
echo ==========================================

REM Check for emscripten
where emcc >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo Error: Emscripten not found. Please install and activate emsdk:
    echo   git clone https://github.com/emscripten-core/emsdk.git
    echo   cd emsdk
    echo   emsdk install latest
    echo   emsdk activate latest
    echo   emsdk_env.bat
    exit /b 1
)

REM Create output directory
if not exist "%WASM_DIR%" mkdir "%WASM_DIR%"

REM Compile to WebAssembly
echo Compiling C++ to WebAssembly...
emcc -O3 -std=c++17 ^
    -I"%SCRIPT_DIR%include" ^
    "%SCRIPT_DIR%src\monte_carlo.cpp" ^
    "%SCRIPT_DIR%src\bindings.cpp" ^
    -o "%WASM_DIR%\monte_carlo.js" ^
    -s WASM=1 ^
    -s MODULARIZE=1 ^
    -s EXPORT_NAME="createMonteCarloModule" ^
    -s ENVIRONMENT='node,web' ^
    -s ALLOW_MEMORY_GROWTH=1 ^
    -s INITIAL_MEMORY=16777216 ^
    -s STACK_SIZE=8388608 ^
    -s EXPORT_ES6=0 ^
    -s EXPORTED_RUNTIME_METHODS='["ccall","cwrap"]' ^
    --bind

if %ERRORLEVEL% neq 0 (
    echo Build failed!
    exit /b 1
)

echo ==========================================
echo Build complete!
echo Output files:
echo   %WASM_DIR%\monte_carlo.js
echo   %WASM_DIR%\monte_carlo.wasm
echo ==========================================