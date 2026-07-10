#!/usr/bin/env bash
# Builds Liters.xcframework + Swift bindings for iOS.
#
# Prereqs:
#   rustup target add aarch64-apple-ios aarch64-apple-ios-sim x86_64-apple-ios
#   Xcode command line tools
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=target/apple
BINDINGS=$OUT/swift
DEVICE_TARGET=aarch64-apple-ios
SIM_TARGETS=(aarch64-apple-ios-sim x86_64-apple-ios)

for t in "$DEVICE_TARGET" "${SIM_TARGETS[@]}"; do
  cargo build -p liters-ffi --release --target "$t"
done

# Generate Swift bindings from the host library's embedded metadata.
cargo build -p liters-ffi --release
rm -rf "$BINDINGS" && mkdir -p "$BINDINGS"
cargo run -p liters-ffi --bin uniffi-bindgen -- generate \
  --library target/release/libliters_ffi.dylib \
  --language swift --out-dir "$BINDINGS"

# Headers directory for the xcframework: the C header + module map.
HEADERS=$OUT/headers
rm -rf "$HEADERS" && mkdir -p "$HEADERS"
cp "$BINDINGS"/*.h "$HEADERS"/
# uniffi emits a .modulemap; xcodebuild wants module.modulemap
cp "$BINDINGS"/*.modulemap "$HEADERS"/module.modulemap

# Fat simulator library.
mkdir -p "$OUT/sim"
lipo -create \
  $(for t in "${SIM_TARGETS[@]}"; do echo "target/$t/release/libliters_ffi.a"; done) \
  -output "$OUT/sim/libliters_ffi.a"

rm -rf "$OUT/Liters.xcframework"
xcodebuild -create-xcframework \
  -library "target/$DEVICE_TARGET/release/libliters_ffi.a" -headers "$HEADERS" \
  -library "$OUT/sim/libliters_ffi.a" -headers "$HEADERS" \
  -output "$OUT/Liters.xcframework"

echo "xcframework: $OUT/Liters.xcframework"
echo "swift sources: $BINDINGS/*.swift (add to your SPM target alongside the xcframework)"
