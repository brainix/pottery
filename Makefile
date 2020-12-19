# --------------------------------------------------------------------------- #
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


# Determine this Makefile's path.  Be sure to place the following line before
# include directives, if any.
THIS_FILE := $(lastword $(MAKEFILE_LIST))


venv ?= venv

init upgrade: formulae := {openssl,readline,xz,redis}
python upgrade: version ?= 3.9.1
upgrade: requirements ?= requirements-to-freeze.txt

clean-redis clean-redis-2: keys_to_delete = \
	basket \
	dilberts \
	expensive-function-cache \
	google-searches \
	letters \
	magic \
	my-counter \
	nextid:user-ids \
	printer \
	search-results \
	squares \
	tel


.PHONY: install
install: init python


.PHONY: init
init:
	-xcode-select --install
	command -v brew >/dev/null 2>&1 || \
		ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
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
	-heroku update
ifneq ($(recursive),)
	rm -rf $(venv)
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
endif
	source $(venv)/bin/activate && \
		pip3 install --upgrade --no-cache-dir pip wheel && \
		pip3 install --requirement $(requirements) --upgrade --no-cache-dir && \
		pip3 freeze > requirements.txt
	git status
	git diff


.PHONY: test
test:
ifeq ($(tests),)
	$(eval $@_SOURCE_FILES := $(shell find . -name '*.py' -not -path './build/*' -not -path './dist/*' -not -path './pottery.egg-info/*' -not -path './venv/*'))
	source $(venv)/bin/activate && \
		coverage3 run -m unittest discover --start-directory tests --verbose && \
		coverage3 report && \
		echo Running static type checks && \
		mypy --no-incremental && \
		echo Running Flake8 on $($@_SOURCE_FILES) && \
		flake8 $($@_SOURCE_FILES) && \
		echo Running isort on $($@_SOURCE_FILES) && \
		isort $($@_SOURCE_FILES) --check-only --diff
else
	source $(venv)/bin/activate && \
		python3 -m unittest --verbose $(tests)
endif

.PHONY: test-readme
test-readme: clean-redis run-doctest clean-redis-2

.PHONY: clean-redis clean-redis-2
clean-redis clean-redis-2:
	@source $(venv)/bin/activate && \
		python3 -c "from redis import Redis; redis = Redis(); redis.delete($(foreach key,$(keys_to_delete),'$(key)',))"

.PHONY: run-doctest
run-doctest:
	source $(venv)/bin/activate && python3 -m doctest -v README.md


.PHONY: release
release:
	rm -f dist/*
	source $(venv)/bin/activate && \
		python3 setup.py sdist && \
		python3 setup.py bdist_wheel && \
		twine upload dist/*

.PHONY: clean
clean:
	rm -rf {$(venv),.coverage,.mypy_cache,build/*,dist/*}


.PHONY: lines-of-code
lines-of-code:
	find . -name '*.py' -not -path "./venv/*" -not -path "./build/*" | xargs wc -l
