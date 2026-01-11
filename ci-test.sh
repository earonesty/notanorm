#!/bin/bash
set -e

make requirements

make lint

make test

make test-mysql

make test-postgres

# test pymysql
python3 -mvenv env-pymysql
. ./env-pymysql/bin/activate || . ./env-pymysql/Scripts/activate

make requirements-pymysql
make test-mysql
