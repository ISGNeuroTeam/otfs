
define ANNOUNCE_BODY
Required section:
 build - build project into build directory, with configuration file and environment
 clean - clean all addition file, build directory and output archive file
 test - run all tests
 pack - make output archive
Addition section:
 package - sbt assembly
 publish - make local repository
endef


GENERATE_VERSION = $(shell grep version build.sbt  | sed -r 's/version := "(.+?)"/\1/' )
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')
GENERATE_SCALA_VERSION = $(shell grep scalaVersion build.sbt  | sed -r 's/scalaVersion := "([0-9]+?\.[0-9]+?)\.[0-9]+"/\1/' )
GENERATE_PROJECT_NAME = $(shell grep name build.sbt  | sed -r 's/name := "(.+?)"/\1/' )
GENERATE_PROJECT_NAME_LOW_CASE = $(shell grep name build.sbt  | sed -r 's/name := "(.+?)"/\1/' | tr A-Z a-z)


SET_VERSION = $(eval VERSION=$(GENERATE_VERSION))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))
SET_SCALA_VERSION = $(eval SCALA_VERSION=$(GENERATE_SCALA_VERSION))
SET_PROJECT_NAME = $(eval PROJECT_NAME=$(GENERATE_PROJECT_NAME))
SET_PROJECT_NAME_LOW_CASE = $(eval PROJECT_NAME_LOW_CASE=$(GENERATE_PROJECT_NAME_LOW_CASE))



.SILENT:

COMPONENTS :=

export ANNOUNCE_BODY
all:
	echo "$$ANNOUNCE_BODY"


package:
	# Addition section
	echo Package
	sbt package

pack: clean test build
	$(SET_VERSION)
	$(SET_BRANCH)
	$(SET_PROJECT_NAME_LOW_CASE)
	rm -f $(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz
	echo Create archive \"$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz\"
	cd build; tar czf ../$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz .

build: package
	# required section
	echo Build
	$(SET_VERSION)
	$(SET_SCALA_VERSION)
	$(SET_PROJECT_NAME)
	$(SET_PROJECT_NAME_LOW_CASE)
	mkdir build
	mkdir build/$(PROJECT_NAME)
	#cp target/scala-$(SCALA_VERSION)/$(PROJECT_NAME_LOW_CASE)_$(SCALA_VERSION)-$(VERSION).jar build/$(PROJECT_NAME)
	cp target/$(PROJECT_NAME_LOW_CASE)-$(VERSION).jar build/$(PROJECT_NAME)
	cp README.md build/$(PROJECT_NAME)/
	cp CHANGELOG.md build/$(PROJECT_NAME)/
	#cp LICENSE.md build/$(PROJECT_NAME)/

clean:
	# required section
	$(SET_VERSION)
	$(SET_SCALA_VERSION)
	$(SET_PROJECT_NAME_LOW_CASE)
	rm -rf $(PROJECT_NAME_LOW_CASE)-*.tar.gz build publish target

test:
	# required section
	echo "Testing..."
	sbt test
