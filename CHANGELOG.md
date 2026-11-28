# Changelog

## [0.1.0](https://github.com/bbmumford/tursokv/compare/v0.0.3...v0.1.0) (2026-11-28)


### ⚠ BREAKING CHANGES

* the value tables' key column and every index are lowerCamelCase

### Bug Fixes

* an Atomic call that keeps conflicting runs alone from its fourth run, so contention among Atomic calls never spends its budget ([f832542](https://github.com/bbmumford/tursokv/commit/f832542d9207ca5e41c413376b6ce70b3e2c6eb0))
* Atomic retries the engine's commit-dependency abort, which asks for the whole transaction to run again ([c1ad682](https://github.com/bbmumford/tursokv/commit/c1ad682432467cceb5dc241b091f918753f346e7))
* go.mod requires policypool v0.2.0 and filekind v0.2.0, with no local replaces, and Go 1.27.4 ([e8741d7](https://github.com/bbmumford/tursokv/commit/e8741d7007c96606466f4ee4f5028882912014d8))
* move tursogo to v0.8.4, its stable release ([46dc419](https://github.com/bbmumford/tursokv/commit/46dc41921a192bcc0ea845bae4a3d0ccb4922a78))
* run each multi-statement call in one transaction, retried on write-write conflict ([08813f6](https://github.com/bbmumford/tursokv/commit/08813f62371e9048fdf6946ef2f3f2a0fff94702))
* writes after a key's expiry and over another type are never lost ([a9831a2](https://github.com/bbmumford/tursokv/commit/a9831a2b1e2991a3da5229dad0c2bb8bd73b4a09))


### Code Refactoring

* the value tables' key column and every index are lowerCamelCase ([097cbd7](https://github.com/bbmumford/tursokv/commit/097cbd73df18055b2f695e3e4c914b84ffcd4e4c))
