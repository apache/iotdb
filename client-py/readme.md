# introduction
This is an example of how to connect to IoTDB with python, using the thrift rpc interfaces. Things will be a bit different
on Linux or Windows, we will introduce how to operate on the two systems separately.

## Prerequisites
python3.7 or later is preferred.

You have to install Thrift (0.11.0 or later) to compile our thrift file into python code. Below is the official
tutorial of installation:
```
http://thrift.apache.org/docs/install/
```

## Compile
If you have added Thrift executable into your path, you may just run `compile.sh` or `compile.bat`, or you will have to
modify it to set variable `THRIFT_EXE` to point to your executable. This will generate thrift sources under folder `target`,
you can add it to your `PYTHONPATH` so that you would be able to use the library in your code.

## Example
We provided an example of how to use the thrift library to connect to IoTDB in `src\client_example.py`, please read it 
carefully before you write your own code.
