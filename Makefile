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

### KLUDGE ###
# When goconvey is installed with go install as a module dependency, it sets its
# working directory to within $GOPATH/pkg/..., which Go makes read-only by
# default (for a good reason - it's supposed to be immutable). However, goconvey
# wants to write coverage information to that location, and, of course, fails.
# As a kludge, we use this var to make the directory writable.
#
# Update the path when upgrading goconvey's version.
GOCONVEY_COVERAGE_DIR="pkg/mod/github.com/smartystreets/goconvey@v1.6.4/web"

all:
	@echo "Please pick a target:"
	@echo "  make init      - initialize the development environment"
	@echo "  make build     - compile the app"
	@echo "  make install   - compile and install all the binaries"
	@echo "  make test      - run tests"
	@echo "  make gofmt     - format all *.go files"
	@echo "  make goconvey  - start a goconvey session (Crtl-C to exit)"
	@echo "  make godoc     - start a godoc server (Ctrl-C to exit)"
	@echo "  make clean    - delete object files and other temporary files"
	@echo "  make pristine - clean + delete everything created by bootstrap"

init:
	./bootstrap
	/bin/bash -c "source bin/bashrc && cd src && \
		go install github.com/smartystreets/goconvey && \
		go install golang.org/x/lint/golint && \
		go install golang.org/x/tools/cmd/godoc && \
		go install github.com/sergey-a-berezin/gocovcheck && \
		go install github.com/sergey-a-berezin/gocovcheck/jsonread && \
		go install github.com/sergey-a-berezin/gocovcheck/gitbasedversion"
	mkdir -p $(GOCONVEY_COVERAGE_DIR)
	chmod -R u+w $(GOCONVEY_COVERAGE_DIR)  # kluge (see above)
	@echo "Bootstrap done!"

build:
	(source "bin/bashrc"; cd "src"; go build ./...; go install ./stockparfait)

test:
	./runtests

gofmt:
	/bin/bash -c "source bin/bashrc && cd src && gofmt -s -w ."

goconvey:
	/bin/bash -c "source bin/bashrc; cd src; goconvey"

godoc:
	@echo "Starting GoDoc server. Please navigate to http://localhost:6060/pkg/"
	/bin/bash -c "source bin/bashrc; godoc -goroot ."

clean:
	rm -f "src/.coverage"
	rm -f "src/coverage.html"

pristine: clean
	chmod -R u+w "pkg"
	rm -rf "pkg" "bin"
