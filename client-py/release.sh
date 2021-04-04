#!/usr/bin/env bash
rm -Rf build
rm -Rf dist
rm -Rf iotdb_session.egg_info

# (Re-)build generated code
(cd ..; mvn clean generate-sources -pl client-py -am)

# Run Linting
flake8

# Run unit tests
pytest .

# See https://packaging.python.org/tutorials/packaging-projects/
python setup.py sdist bdist_wheel
twine upload --repository pypi dist/*