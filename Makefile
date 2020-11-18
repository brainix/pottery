# --------------------------------------------------------------------------- #
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2020, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
# --------------------------------------------------------------------------- #


venv ?= venv

init: formulae := {openssl,readline,xz,redis}

python upgrade: version ?= 3.9.0


.PHONY: release clean

install: init python

init:
	-xcode-select --install
	command -v brew >/dev/null 2>&1 || \
		ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew analytics off
	brew analytics regenerate-uuid
	-brew install $(formulae)
	-git clone https://github.com/pyenv/pyenv.git ~/.pyenv

python:
	cd ~/.pyenv/plugins/python-build/../.. && git pull
	CFLAGS="-I$(shell brew --prefix openssl)/include -I$(shell brew --prefix readline)/include -g -O2" \
		LDFLAGS="-L$(shell brew --prefix openssl)/lib -L$(shell brew --prefix readline)/lib" \
		pyenv install --skip-existing $(version)
	pyenv rehash
	rm -rf $(venv)
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
	source $(venv)/bin/activate && \
		pip3 install --upgrade --no-cache-dir pip wheel && \
		pip3 install --requirement requirements.txt

upgrade:
ifneq ($(recursive),)
	rm -rf $(venv)
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
endif
	source $(venv)/bin/activate && \
		pip3 install --upgrade --no-cache-dir pip wheel && \
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

readme:
	source $(venv)/bin/activate && \
		python3 -c "from redis import Redis; redis = Redis(); print('Number of Redis keys deleted:', redis.delete('dilberts', 'edible', 'expensive-function-cache', 'google-searches', 'lyrics', 'nextid:user-ids', 'printer', 'raj'))"; \
		python3 -m doctest README.md; \
		python3 -c "from redis import Redis; redis = Redis(); print('Number of Redis keys deleted:', redis.delete('dilberts', 'edible', 'expensive-function-cache', 'google-searches', 'lyrics', 'nextid:user-ids', 'printer', 'raj'))"

release:
	rm -f dist/*
	source $(venv)/bin/activate && \
		python3 setup.py sdist && \
		python3 setup.py bdist_wheel && \
		twine upload dist/*

clean:
	rm -rf {$(venv),.coverage,dist/*}
