# Installing DolphinDB Python API

Before installing the DolphinDB Python API, ensure you have a Python environment set up. If not, it is recommended to use the [Anaconda Distribution](https://www.anaconda.com/products/distribution) to download Python and its common libraries.

## Supported Python Versions

For direct download links for the `dolphindb` packages, see [pypi dolphindb](https://pypi.org/project/dolphindb/1.30.21.1/#files). `dolphindb` runs on the following operating systems:

| Operating System | Supported Python Versions            |
| :--------------- | :----------------------------------- |
| Windows (amd64)  | Python 3.6-3.10                      |
| Linux (x86_64)   | Python 3.6-3.10                      |
| Linux (aarch64)  | Python 3.7-3.10 in conda environment |
| Mac (x86_64)     | Python 3.6-3.10 in conda environment |
| Mac (arm64)      | Python 3.8-3.10 in conda environment  |

**Note:** To use the DolphinDB Python API, also install these library dependencies:

- `future`
- `NumPy 1.18 - 1.23.4`
- `pandas 1.0.0` or higher (version 1.3.0 is not supported)

## Installation

Install the DolphinDB Python API with the following pip command (conda commands are not currently supported):

```
$ pip install dolphindb
```

## FAQ

**Q1: Installation of the DolphinDB Python API failed; Import of the** ***dolphindb*** **package failed even though it is successfully installed.**

It is recommended to install the .whl package (again) by following the steps below:

1. Search for the *dolphindb* wheel that runs on your current operating system (e.g., Linux ARM, Mac ARM, etc.) on [PyPI](https://pypi.org/project/dolphindb/#files). Download the wheel (*.whl* file) to your local system.
2. Enter the following command in the terminal:

```
pip debug --verbose
```

The `Compatible tags` section indicates which distributions are compatible with your system.

3. Rename the downloaded *dolphindb* wheel according to the compatibility tags. For example, the file name for Mac(x86_64) is “dolphindb-1.30.19.2-cp37-cp37m-macosx_10_16_x86_64.whl“. If the compatibility tags show that the system version supported by pip is 10.13, then replace the “10_16“ in the original filename with “10_13“.

4. Install the renamed wheel.

If the installation or import still fails, please post your question on [StackOverflow ](https://stackoverflow.com/questions/tagged/dolphindb)with the “dolphindb“ tag. We will get back to you soon.