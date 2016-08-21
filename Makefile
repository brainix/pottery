#-----------------------------------------------------------------------------#
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2016, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



LDFLAGS := -L/usr/local/opt/readline/lib
CPPFLAGS := -I/usr/local/opt/readline/include
CFLAGS := '-g -O2'

BREW := $(shell command -v brew 2>/dev/null)

version ?= 3.5.2
venv ?= 'pottery'



install: init python

init:
	-xcode-select --install
ifndef BREW
	ruby -e "$(curl -fsSL https://raw.github.com/Homebrew/homebrew/go/install)"
endif
	brew install {readline,pyenv,pyenv-virtualenv,redis}

python:
	pyenv install --skip-existing $(version)
	-pyenv uninstall --force $(venv)
	pyenv virtualenv $(version) $(venv)
	pyenv local $(venv)
	pyenv rehash
	pip3 install --upgrade pip
	pip3 install --requirement requirements.txt

upgrade:
	brew update
	brew upgrade --all
	brew cleanup
	pyenv rehash
	pip3 install --upgrade pip
	pip3 install --requirement requirements-to-freeze.txt --upgrade
	pip3 freeze > requirements.txt
	git status
	git diff

release:
	rm -f dist/*
	python3 setup.py sdist
	python3 setup.py bdist_wheel
	twine upload dist/*
