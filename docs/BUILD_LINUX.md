# Build duta-stratumd on Linux

This guide is for producing the Linux stratum server binary for packaging and deployment.

## Requirements

- Rust toolchain with Cargo

## Build

From the repository root:

```bash
cargo build --release --bin duta-stratumd
```

## Output

The release binary is written to:

```text
target/release/duta-stratumd
```

## Recommended verification

Check the available flags before packaging or deploying:

```bash
./target/release/duta-stratumd --help
```

## Next step

For deployment shape, miner login format, daemon dependency, and operator notes, continue with:

- [DEPLOYMENT.md](./DEPLOYMENT.md)
