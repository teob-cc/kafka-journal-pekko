# Notices and attributions

This repository is a fork of [evolution-gaming/kafka-journal](https://github.com/evolution-gaming/kafka-journal),
maintained independently by Lambda House. The original project is
Copyright (c) 2018 Evolution Gaming and licensed under the MIT license (see [LICENSE](LICENSE)).

Functional parity point with upstream: **v9.1.1** (see `V9_CATCHUP.md`).

## Insourced libraries

The `libs/` module contains source code insourced from the following Evolution
(Evolution Gaming) open-source libraries, all MIT-licensed, Copyright (c) their
respective years by Evolution Gaming. Versions listed are the upstream tags the
sources were captured from.

| Library | Upstream repository | Captured version |
|---|---|---|
| cats-helper | https://github.com/evolution-gaming/cats-helper | 3.12.2 |
| skafka | https://github.com/evolution-gaming/skafka | v20.2.1 |
| scassandra4 | https://github.com/evolution-gaming/scassandra | branch `wip/driver-4-support-attempt-2` @ `8b5e9fd12d3aad2a5c214dae9b303cede6422d4a` (unreleased; replaced the driver-3 scassandra v5.6.0, removed in the driver-4 migration — see `DRIVER4_CATCHUP.md`) |
| sstream | https://github.com/evolution-gaming/sstream | 1.1.0 |
| scache | https://github.com/evolution-gaming/scache | v6.0.1 |
| smetrics | https://github.com/evolution-gaming/smetrics | v2.4.3 (verified identical to v2.4.5 for vendored modules) |
| retry | https://github.com/evolution-gaming/retry | 3.1.0 |
| random | https://github.com/evolution-gaming/random | 1.0.5 |
| resource-pool | https://github.com/evolution-gaming/resource-pool | 1.1.0 |
| cassandra-sync | https://github.com/evolution-gaming/cassandra-sync | 4.0.0 |
| play-json-jsoniter (play-json-tools) | https://github.com/evolution-gaming/play-json-tools | v1.2.3 (verified identical to v1.3.0) |
| pekko-extension-serialization | https://github.com/evolution-gaming/pekko-extension | v1.3.1 (verified identical to v2.0.0) |
| pekko-extension-test-actor | https://github.com/evolution-gaming/pekko-extension | v1.3.1 (verified identical to v2.0.0) |
| hostname | https://github.com/evolution-gaming/hostname | 1.0.0 |
| executor-tools | https://github.com/evolution-gaming/executor-tools | 1.0.5 |
| config-tools | https://github.com/evolution-gaming/config-tools | (transitive capture) |
| nel | https://github.com/evolution-gaming/nel | (transitive capture) |

The insourced sources have been modified (Scala 3 migration, dependency
updates); they are not verbatim copies. All modifications are likewise
distributed under the MIT license.
