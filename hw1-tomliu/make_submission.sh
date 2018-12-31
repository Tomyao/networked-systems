#!/usr/bin/env bash

rm -f submission.zip
cd src
make
cd ..
zip -r submission.zip *
