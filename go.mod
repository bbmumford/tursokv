module github.com/bbmumford/tursokv

go 1.25.0

require (
	github.com/bbmumford/policypool v0.0.0
	turso.tech/database/tursogo v0.8.0-pre.12
)

require (
	github.com/bbmumford/filekind v0.0.0 // indirect
	github.com/ebitengine/purego v0.9.1 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/tursodatabase/turso-go-platform-libs v0.8.0-pre.12 // indirect
	github.com/zeebo/blake3 v0.2.4 // indirect
	golang.org/x/sys v0.44.0 // indirect
)

replace github.com/bbmumford/policypool => ../policypool

replace github.com/bbmumford/filekind => ../filekind
