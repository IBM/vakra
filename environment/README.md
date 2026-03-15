# M3 Benchmark REST API setup

## Prerequisites

Before diving into the setup and usage of the CRAG Mock API, ensure you have the following prerequisites installed and set up on your system:
- Git (for cloning the repository)
- Python 3.10

## Installation Guide

### Setting Up Your Environment

First, clone the repository to your local machine using Git. Then, navigate to the repository directory and install the necessary dependencies:

```
cd apis
pip install -r requirements.txt
```

## Running the API Server

To launch the API server on your local machine, use the following Uvicorn command. This starts a fast, asynchronous server to handle API requests.

```
uvicorn server:app --reload
```

Access the API documentation and test the endpoints at `http://127.0.0.1:8000/docs`.

For custom server configurations, specify the host and port as follows:

```
uvicorn server:app --reload --host [HOST] --port [PORT]
```

## System Requirements

- **Supported OS**: Linux, Windows, macOS
- **Python Version**: 3.10
- See `requirements.txt` for a complete list of Python package dependencies.