# Build duta-stratumd on Linux

This guide is for producing the Linux stratum server binary for packaging and deployment.

## Requirements

- Rust toolchain with Cargo

## Build

From the repository root:

```bash
./scripts/build-linux.sh
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

## Run

```bash
./scripts/run-stratum.sh http://127.0.0.1:19085 127.0.0.1:11001
```

## Next step

For deployment shape, miner login format, daemon dependency, and operator notes, continue with:

- [DEPLOYMENT.md](./DEPLOYMENT.md)
