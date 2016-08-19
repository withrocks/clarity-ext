#!/bin/bash


echo "Running all tests with coverage"
echo "-------------------------"
nosetests -v ./test --with-coverage --cover-html --cover-package=clarity_ext --cover-erase
