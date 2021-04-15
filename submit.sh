#!/bin/bash
mvn spotless:apply
git add .
git commit -m "test"
git push
