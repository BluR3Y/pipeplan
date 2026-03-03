# Recommended Project Structure for `pipeplan`

For a modern, production-grade Python library, the **`src/` layout** is the gold standard. It ensures clean packaging, prevents import errors during testing, and neatly separates your library code from repository metadata.

Here is the ideal directory structure for your project folder:

```
pipeplan-project/
│
├── src/                        # 📦 All actual library code lives here
│   └── pipeplan/               # The root package name
│       ├── __init__.py         # Exposes main API (Pipeline, Task, etc.)
│       ├── core.py             # Base classes, Pipeline, ExecutionContext, Task
│       ├── resources.py        # Resource adapters (JsonAdapter, DB connection templates)
│       ├── transfers.py        # Extract, Load operations
│       ├── transforms.py       # Data transformation logic
│       └── py.typed            # Empty file. Tells IDEs your library has type hints!
│
├── tests/                      # 🧪 Test suite
│   ├── conftest.py             # Shared pytest fixtures (e.g., mock ExecutionContext)
│   ├── test_core.py            # Tests for DAG resolution, task retries, etc.
│   ├── test_resources.py       # Tests for JSON adapter, file reading
│   └── test_transforms.py      # Tests for pandas data manipulation
│
├── examples/                   # 💡 Usage examples for users
│   ├── 01_basic_pipeline.py    # The example.py we created earlier
│   └── 02_custom_plugin.py     # Example showing how to add custom operations
│
├── docs/                       # 📚 Documentation (e.g., Sphinx or MkDocs)
│   ├── index.md
│   └── api_reference.md
│
├── .github/                    # ⚙️ CI/CD (if using GitHub)
│   └── workflows/
│       ├── test.yml            # Auto-runs pytest on every commit
│       └── publish.yml         # Auto-publishes to PyPI on release tags
│
├── pyproject.toml              # 📝 Modern Python dependency & build configuration
├── .gitignore                  # Standard Python gitignore (ignores __pycache__, .env, etc.)
├── README.md                   # Project description, installation, quickstart
└── LICENSE                     # Open source license (MIT, Apache 2.0, etc.)

```

## Deep Dive into Key Files

### 1. `src/pipeplan/__init__.py`

This file makes your library user-friendly. Instead of forcing users to remember which submodule a class lives in, you import the most common tools here so they can be accessed at the top level.

```
# src/pipeplan/__init__.py
from .core import Pipeline, Task, ExecutionContext
from .resources import ResourceOps, FileResource, DatabaseResource
from .transfers import TransferOps
from .transforms import TransformOps

__all__ = [
    "Pipeline",
    "Task",
    "ExecutionContext",
    "ResourceOps",
    "FileResource",
    "DatabaseResource",
    "TransferOps",
    "TransformOps",
]
__version__ = "0.1.0"

```

*Usage impact:* Users can now just do `from pipeplan import Pipeline, Task` instead of `from pipeplan.core import Pipeline, Task`.

### 2. `pyproject.toml`

Since you are using modern tools and `importlib.metadata` for plugin entry points, you should use `pyproject.toml` instead of the legacy `setup.py`. Here is how you configure it (using `setuptools` or `hatchling` as a build backend):

```
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "pipeplan"
version = "0.1.0"
description = "A robust, DAG-based data pipeline library."
readme = "README.md"
requires-python = ">=3.8"
dependencies = [
    "pandas>=2.0.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov>=4.0",
    "mypy>=1.0",
]

# 🔌 This is how you register YOUR built-in operations using the 
# entry_points logic we built into Operation._load_plugins()!
[project.entry-points."pipeplan.resource.file"]
json = "pipeplan.resources:JsonAdapter"

[project.entry-points."pipeplan.transfer"]
extract = "pipeplan.transfers:_extract"
load = "pipeplan.transfers:_load"

```

### 3. `src/pipeplan/py.typed`

This is an incredibly simple but important file. It's literally just an empty file named `py.typed`. By including it, you signal to type-checkers (like `mypy` or the Pylance extension in VS Code) that your library includes type hints. This gives anyone using your library robust autocomplete and type warnings.

### 4. `tests/conftest.py`

This is where `pytest` looks for reusable testing logic. Because your pipeline now relies on an `ExecutionContext`, you can create a fixture here that spins up a clean context for every single test automatically, preventing test pollution.

## Summary of Next Steps for Setup

1. **Initialize the repo:** `git init`
2. **Create the directories:** `mkdir -p src/pipeplan tests examples docs`
3. **Move the code:** Place the files we generated (`core.py`, `resources.py`, etc.) into `src/pipeplan/`.
4. **Install locally:** Run `pip install -e .[dev]` from the root folder. This installs your library in "editable" mode so you can test it locally, along with your development dependencies like `pytest`.