# --------------------------------------------------------------------------- #
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2026, Rajiv Bakulesh Shah, original author.              #
#                                                                             #
#   Licensed under the Apache License, Version 2.0 (the "License");           #
#   you may not use this file except in compliance with the License.          #
#   You may obtain a copy of the License at:                                  #
#       http://www.apache.org/licenses/LICENSE-2.0                            #
#                                                                             #
#   Unless required by applicable law or agreed to in writing, software       #
#   distributed under the License is distributed on an "AS IS" BASIS,         #
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  #
#   See the License for the specific language governing permissions and       #
#   limitations under the License.                                            #
# --------------------------------------------------------------------------- #


# Determine this Makefile's path.  Be sure to place the following line before
# include directives, if any.
THIS_FILE := $(lastword $(MAKEFILE_LIST))


venv ?= venv

init upgrade: formulae := {openssl,readline,xz,redis}
python upgrade: version ?= 3.14.2
upgrade: requirements ?= requirements-to-freeze.txt
delete-keys: pattern ?= tmp:*


.PHONY: install
install: init python


.PHONY: init
init:
	-xcode-select --install
	# command -v brew >/dev/null 2>&1 || \
	# 	ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew analytics regenerate-uuid
	brew analytics off
	-brew install $(formulae)
	-git clone https://github.com/pyenv/pyenv.git ~/.pyenv

.PHONY: python
python:
	cd ~/.pyenv/plugins/python-build/../.. && git pull
	CFLAGS="-I$(shell brew --prefix openssl)/include -I$(shell brew --prefix readline)/include -g -O2" \
		LDFLAGS="-L$(shell brew --prefix openssl)/lib -L$(shell brew --prefix readline)/lib" \
		pyenv install --skip-existing $(version)
	pyenv rehash
	@$(MAKE) --makefile=$(THIS_FILE) upgrade recursive=true requirements=requirements.txt

.PHONY: upgrade
upgrade:
	-brew update
	-brew upgrade $(formulae)
	brew cleanup
ifneq ($(recursive),)
	rm -rf $(venv)
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
endif
	source $(venv)/bin/activate && \
		pip3 install --upgrade --no-cache-dir pip setuptools wheel && \
		pip3 install --requirement $(requirements) --upgrade --no-cache-dir && \
		pip3 freeze > requirements.txt
	git status
	git diff


.PHONY: test
test:
	$(eval $@_SOURCE_FILES := $(shell find . -name '*.py' -not -path './.git/*' -not -path './build/*' -not -path './dist/*' -not -path './pottery.egg-info/*' -not -path './venv/*'))
	source $(venv)/bin/activate && \
		pytest --verbose --cov-config=.coveragerc --cov=pottery --cov=tests && \
		echo Running static type checks && \
		mypy && \
		echo Running Flake8 on $($@_SOURCE_FILES) && \
		flake8 $($@_SOURCE_FILES) --count --max-complexity=10 --statistics && \
		echo Running isort on $($@_SOURCE_FILES) && \
		isort $($@_SOURCE_FILES) --check-only --diff && \
		bandit --recursive pottery && \
		safety scan


.PHONY: release
release:
	rm -f dist/*
	source $(venv)/bin/activate && \
		python3 setup.py sdist && \
		python3 setup.py bdist_wheel && \
		twine upload dist/*


# Usage:
#	make pattern="tmp:*" delete-keys
.PHONY: delete-keys
delete-keys:
	redis-cli --scan --pattern "$(pattern)" | xargs redis-cli del

.PHONY: clean
clean:
	rm -rf {$(venv),pottery/__pycache__,tests/__pycache__,.coverage,.mypy_cache,pottery.egg-info,build,dist}


.PHONY: lines-of-code
lines-of-code:
	find . -name '*.py' -not -path "./.git/*" -not -path "./venv/*" -not -path "./build/*" | xargs wc -l
