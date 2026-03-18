# duta-stratumd

`duta-stratumd` is the DUTA public stratum server.

It sits between miners and the daemon mining API, handles worker sessions, distributes jobs, validates shares, and submits block candidates upstream.

Current release line: `1.0.0`

Website: https://dutago.xyz

## Repository scope

This repository includes:

- the stratum listener
- miner login and worker session handling
- job distribution
- share validation
- block candidate submission to the daemon mining API
- optional bridge hooks for pool-side accounting systems

This repository does not include:

- chain consensus
- wallet storage
- block explorer features
- full pool website functionality

## Main binary

- `duta-stratumd`

## Release position

This repo is for operators who want to run a public DUTA stratum endpoint or connect the stratum layer to an external pool stack.

## Documentation

- Linux build: [docs/BUILD_LINUX.md](./docs/BUILD_LINUX.md)
- Windows build: [docs/BUILD_WINDOWS.md](./docs/BUILD_WINDOWS.md)
- Deployment guide: [docs/DEPLOYMENT.md](./docs/DEPLOYMENT.md)
- Install from binary: [docs/INSTALL_FROM_BINARY.md](./docs/INSTALL_FROM_BINARY.md)
