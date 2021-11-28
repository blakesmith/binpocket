all: src/manifest_generated.rs

src/manifest_generated.rs: fbs/manifest.fbs
	flatc -o src -I fbs/ -r fbs/manifest.fbs
