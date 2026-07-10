#!/usr/bin/env bash
# Builds Android JNI libraries + Kotlin bindings.
#
# Prereqs:
#   cargo install cargo-ndk
#   rustup target add aarch64-linux-android armv7-linux-androideabi x86_64-linux-android
#   ANDROID_NDK_HOME pointing at an NDK
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=target/android
ABIS=(arm64-v8a armeabi-v7a x86_64)

cargo ndk $(for a in "${ABIS[@]}"; do echo -t "$a"; done) \
  -o "$OUT/jniLibs" build -p liters-ffi --release

# Kotlin bindings from the host library's embedded metadata.
cargo build -p liters-ffi --release
rm -rf "$OUT/kotlin" && mkdir -p "$OUT/kotlin"
cargo run -p liters-ffi --bin uniffi-bindgen -- generate \
  --library target/release/libliters_ffi.dylib \
  --language kotlin --out-dir "$OUT/kotlin"

echo "jni libs: $OUT/jniLibs/{arm64-v8a,armeabi-v7a,x86_64}/libliters_ffi.so"
echo "kotlin sources: $OUT/kotlin (package into your AAR; requires JNA)"
