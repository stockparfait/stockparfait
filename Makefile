# Copyright 2022 Stock Parfait

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


GOPATH=gopath
# Space separated list of package paths.
PACKAGES=./stockparfait
INSTALLS=./stockparfait

all:
	@echo "Please pick a target:"
	@echo "  make init     - initialize the development environment"
	@echo "  make install  - compile and install all the binaries"
	@echo "  make test     - run tests"
	@echo "  make gofmt    - format all *.go files"
	@echo "  make goconvey - start a goconvey session (Crtl-C to exit)"
	@echo "  make clean    - delete object files and other temporary files"
	@echo "  make pristine - clean + delete everything created by bootstrap"

init:
	./bootstrap
	/bin/bash -c "source $(GOPATH)/bin/bashrc && \
		go install github.com/smartystreets/goconvey@v1.7.2 && \
		go install honnef.co/go/tools/cmd/staticcheck@2021.1.2 && \
		go install github.com/sergey-a-berezin/gocovcheck/gocovcheck@v1.2.0 && \
		go install github.com/sergey-a-berezin/gocovcheck/jsonread@v1.2.0 && \
		go install github.com/sergey-a-berezin/gocovcheck/gitbasedversion@v1.2.0"
	@echo "Bootstrap done!"

install:
	(source "$(GOPATH)/bin/bashrc"; go install $(INSTALLS))

test:
	./runtests $(PACKAGES)

gofmt:
	/bin/bash -c "source $(GOPATH)/bin/bashrc && gofmt -s -w $(PACKAGES)"

goconvey:
	/bin/bash -c "source $(GOPATH)/bin/bashrc; goconvey -excludedDirs gopath"

clean:
	rm -f ".coverage"
	rm -f "coverage.html"

pristine: clean
	chmod -R u+w "$(GOPATH)/pkg"
	rm -rf "$(GOPATH)"
