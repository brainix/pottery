#-----------------------------------------------------------------------------#
#   Makefile                                                                  #
#                                                                             #
#   Copyright © 2015-2017, Rajiv Bakulesh Shah, original author.              #
#   All rights reserved.                                                      #
#-----------------------------------------------------------------------------#



python: CFLAGS := '-I$(brew --prefix readline)/include -g -O2'
python: LDFLAGS := -L$(brew --prefix readline)/lib

version ?= 3.5.2
venv ?= 'pottery'



install: init python

init:
	-xcode-select --install
	command -v brew >/dev/null 2>&1 || ruby -e "$$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
	brew analytics off
	brew analytics regenerate-uuid
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
	brew upgrade
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
