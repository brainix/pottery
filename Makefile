#-----------------------------------------------------------------------------#
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



python: CFLAGS := "-I$(brew --prefix readline)/include -g -O2"
python: LDFLAGS := -L$(brew --prefix readline)/lib

version ?= 3.5.2
venv ?= venv



install: init python

init:
	-xcode-select --install
	command -v brew >/dev/null 2>&1 || \
		ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew analytics off
	brew analytics regenerate-uuid
	brew install {openssl,readline,xz,pyenv,redis}

python:
	pyenv install --skip-existing $(version)
	pyenv rehash
	~/.pyenv/versions/$(version)/bin/python3 -m venv $(venv)
	source $(venv)/bin/activate && \
		pip3 install --upgrade pip && \
		pip3 install --requirement requirements.txt

upgrade:
	brew update
	brew upgrade
	brew cleanup
	pyenv rehash
	source $(venv)/bin/activate && \
		pip3 install --upgrade pip && \
		pip3 install --requirement requirements-to-freeze.txt --upgrade && \
		pip3 freeze > requirements.txt
	git status
	git diff

test:
ifeq ($(tests),)
	source $(venv)/bin/activate && \
		coverage3 run -m unittest discover --start-directory tests --verbose && \
		coverage3 report
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
