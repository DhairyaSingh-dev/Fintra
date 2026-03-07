# Makefile for compiling Monte Carlo engine to WebAssembly
# Requires: Emscripten SDK (emsdk)

CXX = emcc
CXXFLAGS = -O3 -std=c++17 -fPIC

EMFLAGS = \
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

SRC_DIR = src
INCLUDE_DIR = include
BUILD_DIR = ../static/wasm

SOURCES = $(SRC_DIR)/monte_carlo.cpp $(SRC_DIR)/bindings.cpp
OUTPUT = $(BUILD_DIR)/monte_carlo

all: dirs $(OUTPUT).js $(OUTPUT).wasm

dirs:
	@mkdir -p $(BUILD_DIR)

$(OUTPUT).js $(OUTPUT).wasm: $(SOURCES)
	$(CXX) $(CXXFLAGS) $(EMFLAGS) \
	-I$(INCLUDE_DIR) \
	$(SOURCES) \
	-o $(OUTPUT).js

clean:
	rm -f $(BUILD_DIR)/monte_carlo.js $(BUILD_DIR)/monte_carlo.wasm

.PHONY: all clean dirs