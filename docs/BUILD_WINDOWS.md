# Build duta-stratumd on Windows

This guide is for producing the Windows stratum server binary for packaging and deployment.

## Requirements

- Rust toolchain with Cargo
- Visual Studio C++ build tools or an equivalent MSVC-capable Rust environment

## Build

From the repository root:

```powershell
cargo build --release --bin duta-stratumd
```

## Output

The release binary is written to:

```text
target\release\duta-stratumd.exe
```

## Recommended verification

Check the available flags before packaging or deploying:

```powershell
.\target\release\duta-stratumd.exe --help
```

## Next step

For deployment shape, miner login format, daemon dependency, and operator notes, continue with:

- [DEPLOYMENT.md](./DEPLOYMENT.md)
