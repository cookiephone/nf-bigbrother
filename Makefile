# Use the Gradle wrapper by default; override with e.g. `make GRADLE=gradle ...`
GRADLE ?= ./gradlew

# Build the plugin
assemble:
	$(GRADLE) assemble

clean:
	rm -rf .nextflow*
	rm -rf work
	rm -rf build
	$(GRADLE) clean

# Run plugin unit tests
test:
	$(GRADLE) test

# Install the plugin into the local Nextflow plugins dir
install:
	$(GRADLE) install

# Full start-to-finish test: run the validation pipeline and check its graph
VERSION := $(shell sed -n "s/^version = '\(.*\)'/\1/p" build.gradle)
e2e: install
	cd validation && rm -rf bb_out work .nextflow* && \
	  nextflow run . -plugins nf-bigbrother@$(VERSION) -ansi-log false && \
	  python3 assert_dag.py bb_out

# Publish the plugin to the Nextflow registry
release:
	$(GRADLE) releasePlugin
