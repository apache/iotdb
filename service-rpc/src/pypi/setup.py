import setuptools
import io


try:
    with io.open('README.md', encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''


print long_description

setuptools.setup(
    name="apache-iotdb", # Replace with your own username
    version="0.9.0",
    author=" Apache Software Foundation",
    author_email="dev@iotdb.apache.org",
    description="Apache IoTDB (incubating) client API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/apache/incuabtor-iotdb",
    packages=setuptools.find_packages(),
    install_requires=[
              'thrift',
          ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires='>=3.7',
    license='Apache License, Version 2.0',
)