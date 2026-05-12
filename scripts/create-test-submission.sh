#!/bin/bash
# Creates a test submission tar.gz from the example engine

set -e

OUTPUT="${1:-/tmp/test-submission.tar.gz}"

echo "Creating test submission from examples/rust-starter..."

cd examples/rust-starter
tar czf "$OUTPUT" \
    Cargo.toml \
    Dockerfile \
    README.md \
    src/

echo "✓ Test submission created: $OUTPUT"
echo "  Size: $(du -h "$OUTPUT" | cut -f1)"

# Verify it's valid gzip
if file "$OUTPUT" | grep -q gzip; then
    echo "✓ Valid gzip format"
else
    echo "✗ Invalid format!"
    exit 1
fi