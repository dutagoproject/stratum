# duta-stratumd 0.0.1-beta

This is the first public beta release of the DUTA stratum server.
It is for operators who want to run a public or private stratum endpoint against a DUTA node.

## Highlights

- Public stratum listener for DUTA miners
- Job distribution and share validation
- Upstream block submission to a DUTA daemon

## Included files

- `duta-stratumd`

## Who should use this

Use this package if you want to:

- accept miner connections
- distribute mining jobs
- validate shares
- bridge accepted work to a DUTA daemon

## Quick start

1. Extract the archive.
2. Configure the upstream DUTA daemon endpoint.
3. Review bind and policy settings.
4. Start `duta-stratumd`.
5. Test login, job assignment, and submit flow with a known miner.

## Security notes

- Run the stratum service separately from admin RPC.
- Keep the upstream daemon admin interface private.
- Review rate limits, per-IP policy, and bind settings before opening the service publicly.
- Use a reverse proxy only for web-facing pool services, not for raw stratum traffic.

## Checksums and archives

Choose the archive that matches your platform:

- Linux x86_64
- Windows x86_64

If a checksum file is attached to the release, verify it before deployment.

## Notes for this beta

This release is intended for operators who are comfortable running public mining infrastructure and reviewing logs, bind settings, and upstream node health.

For deployment and security notes, see the repository documentation.
