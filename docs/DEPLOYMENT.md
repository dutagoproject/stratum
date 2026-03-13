# duta-stratumd Deployment Guide

This guide covers the public operator-facing behavior of the DUTA stratum server.

## Role in the stack

`duta-stratumd` does not produce block templates by itself.
It depends on a DUTA node mining endpoint and distributes jobs to miners over stratum.

Typical flow:

1. `dutad` exposes a mining HTTP listener
2. `duta-stratumd` fetches work from that listener
3. miners connect to `duta-stratumd`
4. accepted block candidates are submitted back to the node mining API

## Important flags

- `--bind`
  Stratum bind address. Default: `0.0.0.0:11001`
- `--daemon`
  Base URL for the DUTA mining API. Default: `http://127.0.0.1:19085`
- `--share-bits`
  Pool-side share difficulty target. Default: `24`
- `--job-refresh-secs`
  Work refresh timeout. Default: `15`
- `--job-ttl-secs`
  Job retention window. Default: `30`
- `--network`
  Optional explicit network label such as `mainnet` or `testnet`

## Mainnet example

```bash
./duta-stratumd \
  --bind 0.0.0.0:11001 \
  --daemon http://127.0.0.1:19085 \
  --share-bits 24 \
  --network mainnet
```

## Testnet example

```bash
./duta-stratumd \
  --bind 0.0.0.0:11001 \
  --daemon http://127.0.0.1:18085 \
  --share-bits 24 \
  --network testnet
```

## Miner login format

Miners log in with:

```text
WALLET_ADDRESS
```

or:

```text
WALLET_ADDRESS.worker-name
```

The wallet must match the selected network.

## Operator notes

- keep the node admin RPC private
- point stratum to the node mining listener, not the admin RPC
- use explicit `--network` if your deployment topology makes inference ambiguous
- choose `share-bits` based on the hashrate class of your miners
- watch logs for non-stale reject reasons and repeated reconnect churn

## Expected logging

Normal logs include:

- miner connected
- new job
- accepted share
- block found
- miner disconnected

Stale reject spam is intentionally suppressed because it is normal around block turnover.

## Common mistakes

### Stratum points to the wrong node port

Use the mining listener, not the node admin RPC.

Mainnet default mining listener:

- `19085`

Testnet default mining listener:

- `18085`

### Wrong network wallet format

If a miner logs in with a wallet from the wrong network, job fetch or share submission will fail.

### Public deployment without basic log review

Before publishing a pool endpoint, confirm that the logs show stable miner connections, clean job refresh, and no persistent non-stale rejects.
