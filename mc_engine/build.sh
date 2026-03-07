#!/bin/bash
# Build script for Monte Carlo WebAssembly module
# Requires Emscripten SDK (emsdk) to be installed and activated

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
WASM_DIR="$PROJECT_ROOT/static/wasm"

echo "=========================================="
echo "Building Monte Carlo WASM Module"
echo "=========================================="

# Check for emscripten
if ! command -v emcc &> /dev/null; then
    echo "Error: Emscripten not found. Please install and activate emsdk:"
    echo "  git clone https://github.com/emscripten-core/emsdk.git"
    echo "  cd emsdk"
    echo "  ./emsdk install latest"
    echo "  ./emsdk activate latest"
    echo "  source ./emsdk_env.sh"
    exit 1
fi

# Create output directory
mkdir -p "$WASM_DIR"

# Compile to WebAssembly
echo "Compiling C++ to WebAssembly..."
emcc -O3 -std=c++17 \
    -I"$SCRIPT_DIR/include" \
    "$SCRIPT_DIR/src/monte_carlo.cpp" \
    "$SCRIPT_DIR/src/bindings.cpp" \
    -o "$WASM_DIR/monte_carlo.js" \
    -s WASM=1 \
    -s MODULARIZE=1 \
    -s EXPORT_NAME="createMonteCarloModule" \
    -s ENVIRONMENT='node,web' \
    -s ALLOW_MEMORY_GROWTH=1 \
    -s INITIAL_MEMORY=16777216 \
    -s STACK_SIZE=8388608 \
    -s EXPORT_ES6=0 \
    -s EXPORTED_RUNTIME_METHODS='["ccall","cwrap"]' \
    --bind

echo "=========================================="
echo "Build complete!"
echo "Output files:"
echo "  $WASM_DIR/monte_carlo.js"
echo "  $WASM_DIR/monte_carlo.wasm"
echo "=========================================="