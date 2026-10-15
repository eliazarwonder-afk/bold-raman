# Contributing to PulseQueue

Thank you for your interest in contributing to **PulseQueue**! We welcome contributions from engineers of all experience levels.

Please review this guide before submitting code or opening a pull request.

---

## 🛠️ Development Setup

PulseQueue requires **Python 3.10+**.

### 1. Fork and Clone the Repository
```bash
git clone https://github.com/<your-username>/pulsequeue.git
cd pulsequeue
```

### 2. Create a Virtual Environment
```bash
# On Linux / macOS
python -m venv .venv
source .venv/bin/activate

# On Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install in Editable Mode with Dev Tooling
```bash
pip install --upgrade pip
pip install -e ".[dev]"
```

---

## 🧪 Verification & Quality Checks

Before submitting any Pull Request, ensure that **all tests pass**, **type checks succeed**, and **code is formatted**:

### Run Test Suite
```bash
pytest -v
```

### Run Ruff (Linter and Formatter)
```bash
# Check lint issues
ruff check .

# Check code formatting
ruff format --check .

# Auto-format code
ruff format .
```

### Run Mypy (Strict Type Checking)
```bash
mypy src
```

---

## 🌿 Git Branching & Commit Conventions

We follow the [Conventional Commits](https://www.conventionalcommits.org/) standard.

### Branch Names
* `feat/<feature-name>` (e.g. `feat/postgres-broker`)
* `fix/<bug-description>` (e.g. `fix/dlq-requeue-crash`)
* `docs/<topic>` (e.g. `docs/add-benchmarks`)

### Commit Messages
Format: `<type>(<scope>): <subject>`

Examples:
* `feat(broker): add rabbitmq amqp broker support`
* `fix(worker): prevent memory leak during long-running tasks`
* `docs(readme): update quickstart with async lifespan pattern`
* `test(retry): add test coverage for decorrelated jitter`

---

## 🚀 Submitting a Pull Request (PR)

1. Ensure your branch is rebased onto the latest `main` branch.
2. Push your changes to your personal GitHub fork.
3. Open a Pull Request against `main`.
4. Fill out the **Pull Request Template** completely.
5. Ensure all GitHub Actions CI checks (test matrix across Python 3.10-3.13, ruff, mypy) are green.
6. A maintainer will review your code within 48 hours!
