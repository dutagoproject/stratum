# Build duta-stratumd on Windows

This guide is for producing the Windows stratum server binary without using PowerShell.

## Requirements

- Rust toolchain with Cargo
- Visual Studio C++ build tools or an equivalent MSVC-capable Rust environment

## Build

From the repository root:

```bat
scripts\build-windows.cmd
```

## Output

The release binary is written to:

```text
target\release\duta-stratumd.exe
```

## Recommended verification

Check the available flags before packaging or deploying:

```bat
target\release\duta-stratumd.exe --help
```

## Run

```bat
scripts\run-stratum.cmd http://127.0.0.1:19085 127.0.0.1:11001
```

## Next step

For deployment shape, miner login format, daemon dependency, and operator notes, continue with:

- [DEPLOYMENT.md](./DEPLOYMENT.md)
