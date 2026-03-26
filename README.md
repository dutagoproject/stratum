# duta-stratumd

`duta-stratumd` is the DUTA public stratum server.

It sits between miners and the daemon mining API, handles worker sessions, distributes jobs, validates shares, and submits block candidates upstream.

Current release line: `1.0.3`

Release `1.0.3` focus:

- submit/session churn handling is more stable under rapid job refresh
- identical wallet work is no longer re-announced as if it were new work
- public peer behavior on the backing daemon side is now hard enough that stratum burn-in can run against a cleaner chain

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
- Historical beta notes: [docs/RELEASE_DOWNLOAD_0.0.1-beta.md](./docs/RELEASE_DOWNLOAD_0.0.1-beta.md)
