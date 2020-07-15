# --------------------------------------------------------------------------- #
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


init upgrade: formulae := {openssl,readline,xz,pyenv,redis}

version ?= 3.8.4
venv ?= venv


install: init python

init:
	-xcode-select --install
	command -v brew >/dev/null 2>&1 || \
		ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew analytics off
	brew analytics regenerate-uuid
	-brew install $(formulae)

python:
	CFLAGS="-I$(shell brew --prefix openssl)/include -I$(shell brew --prefix readline)/include -g -O2" \
		LDFLAGS="-L$(shell brew --prefix openssl)/lib -L$(shell brew --prefix readline)/lib" \
		pyenv install --skip-existing $(version)
	rm -rf $(venv)
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
	source $(venv)/bin/activate && \
		pip3 install --upgrade pip && \
		pip3 install --requirement requirements.txt

upgrade:
	brew update
	-brew upgrade $(formulae)
	brew cleanup
	source $(venv)/bin/activate && \
		pip3 install --upgrade pip && \
		pip3 install --requirement requirements-to-freeze.txt --upgrade --no-cache-dir && \
		pip3 freeze > requirements.txt
	git status
	git diff

test:
ifeq ($(tests),)
	$(eval $@_SOURCE_FILES := $(shell find . -name '*.py' -not -path './build/*' -not -path './dist/*' -not -path './pottery.egg-info/*' -not -path './venv/*'))
	source $(venv)/bin/activate && \
		coverage3 run -m unittest discover --start-directory tests --verbose && \
		coverage3 report && \
		echo Running Flake8 on $($@_SOURCE_FILES) && \
		flake8 $($@_SOURCE_FILES) && \
		echo Running isort on $($@_SOURCE_FILES) && \
		isort $($@_SOURCE_FILES) --check-only --diff
else
	source $(venv)/bin/activate && \
		python3 -m unittest --verbose $(tests)
endif

release:
	rm -f dist/*
	source $(venv)/bin/activate && \
		python3 setup.py sdist && \
		python3 setup.py bdist_wheel && \
		twine upload dist/*

clean:
	rm -rf {$(venv),.coverage,dist/*}
