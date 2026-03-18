# Install duta-stratumd from Binary Bundles

This guide is for operators who want to run the public stratum binary from a release bundle instead of building from source.

It covers:

- `duta-stratumd`

## What to download

For each final release, download the bundle that matches your platform:

- Windows: `duta-stratum-<version>-windows-x86_64`
- Linux: `duta-stratum-<version>-linux-x86_64`

Also keep:

- `manifest.json`
- `sha256sums.txt`

## Verify the checksums

On Linux:

```bash
sha256sum -c sha256sums.txt
```

On Windows:

```bat
certutil -hashfile duta-stratumd.exe SHA256
```

## Linux install example

```bash
tar -xzf duta-stratum-1.0.0-linux-x86_64.tar.gz
cd duta-stratum-1.0.0-linux-x86_64
install -m 0755 duta-stratumd /usr/local/bin/duta-stratumd
```

## Windows install example

Extract the ZIP archive into a folder you control, for example:

```text
C:\DUTA\stratum
```

Then verify:

```bat
duta-stratumd.exe --help
```

## Suggested layout

Linux:

```text
/usr/local/bin/duta-stratumd
```

Windows:

```text
C:\DUTA\stratum\duta-stratumd.exe
```

## Important notes

- point stratum to the daemon mining listener, not the admin RPC
- use a public bind only if you really intend to expose the endpoint
- verify logs after deployment for stable login, work refresh, and accepted shares
