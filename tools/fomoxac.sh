#!/bin/sh
set -e
printf 'module %s\n' "$1" > go.mod
trap 'rm -f go.mod' EXIT
fomoxac generate -q
