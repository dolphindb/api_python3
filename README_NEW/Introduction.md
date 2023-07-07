# Introduction

The DolphinDB API allows seamless data transfer and script execution between the DolphinDB server and Python client. Using the DolphinDB API, you can leverage DolphinDB's powerful computing and storage capabilities to manipulate, analyze and model data within a Python environment. The "dolphindb" package is the name of the DolphinDB Python API.

This tutorial has three main chapters:

- Quick Start - includes installing the DolphinDB Python API, a simple demo, and some common operations to get you up and running quickly.
- Basic Operations -introduces the core classes and methods for interacting with DolphinDB, including creating sessions and connection pools, appending data, subscribing to stream data, writing to the server asynchronously, etc.
- Advanced Operations - provides a deep dive into more advanced topics, including type conversion between DolphinDB and Python, comparing methods for uploading and writing data, advanced stream subscription, object-oriented operations on databases and table, and other features.

The DolphinDB Python API provides various methods for connecting to DolphinDB servers, executing scripts, and sending messages. It supports batch processing and asynchronous execution. The API transfers different data types between DolphinDB and Python, including pandas.DataFrames and arrow.Tables.

The DolphinDB Python API is compatible with Python 3.6 and above on Linux (x86_64, arm), Windows, and macOS (arm64, x86_64). It utilizes Pybind11 to develop a C++ library that optimizes multithreaded processing in the background for maximum performance.

You can post any questions about the DolphinDB Python API on [Stack Overflow with the "dolphindb" tag](https://stackoverflow.com/questions/tagged/dolphindb). We actively monitor Stack Overflow and will respond to your questions promptly.
