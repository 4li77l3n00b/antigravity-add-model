# Origin and packaging changes

This component packages original user-owned ACP transport, gateway compatibility,
and native Swarm integration code for inclusion in a fork of
`vahapogut/antigravity-add-model`. It is offered under the same Apache-2.0 license.
The license text in this directory is an unchanged copy of the target repository's
LICENSE, Git blob `261eeb9e9f8b2b4b0d119366dda99c6fd7d35c64` (GitHub license API).
Retain the target repository's own notices when merging.

Packaging changes are limited to relocatable paths, a copying user installer,
independent gateway configuration, isolated tests, and standalone documentation.
Persistent bridge, native-deletion and native-Swarm wire contracts are retained.
No vendor source or binary is included. Test fixtures are synthetic and created
at runtime; no prior execution reports, private settings or session data ship.

Directory roles:

- `bridge/`: persistent transport, native deletion, configuration helper, fixtures.
- `gateway/`: optional local effort-alias relay and independent launcher.
- `swarm/`: native engine facade, read-only store and original agent configuration.
- `tests/`: unit/transport/installation checks without a vendor binary.
- `install.py`: copies runtime files; does not download software or start services.

The compatibility namespace `maichart` is an API identifier only. There is no
runtime dependency on its former workspace, datasets, user home, or IDE auth.
