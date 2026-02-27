# Contributing to GreenPulse India 🌿

First off — thank you for considering contributing! GreenPulse India thrives because of community members like you.

## 🚦 Before You Start

- Check [existing issues](https://github.com/your-org/GreenPulse-India/issues) to avoid duplicate work
- For major changes, open an issue first to discuss your approach
- Make sure your contribution aligns with the project's goals

## 🛠️ Development Setup

```bash
# 1. Fork and clone
git clone https://github.com/YOUR_USERNAME/GreenPulse-India.git
cd GreenPulse-India

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt   # lint + type check tools

# 4. Copy and configure environment
cp .env.example .env

# 5. Start the infrastructure
docker-compose up -d
```

## 📐 Code Style

We follow **PEP 8** with these conventions:

- **Type hints** on all function signatures
- **Docstrings** on all public functions and classes (Google style)
- **Maximum line length:** 100 characters
- **Imports:** stdlib → third-party → local, alphabetical within groups

Run the linter before submitting:

```bash
# Lint
ruff check src/

# Type check
mypy src/ --ignore-missing-imports

# Format
ruff format src/
```

## 🌿 Branch Naming

| Type | Format | Example |
|---|---|---|
| Feature | `feat/short-description` | `feat/add-pm1-sensor` |
| Bug fix | `fix/short-description` | `fix/kafka-reconnect-loop` |
| Docs | `docs/short-description` | `docs/update-api-reference` |
| Refactor | `refactor/short-description` | `refactor/extract-breathe-score` |

## 🔁 Pull Request Process

1. Fork → branch → commit → push → open PR against `main`
2. Fill out the PR template completely
3. Ensure all CI checks pass (linting, type check)
4. Request a review from a maintainer
5. Address review comments and get approval
6. Squash-merge into `main`

## ✅ Commit Message Format

We use [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

Examples:
```
feat(producer): add SO2 sensor reading to city data
fix(pipeline): handle Kafka reconnect after broker restart
docs(readme): add troubleshooting section for Windows
refactor(breathe_score): extract penalty functions
```

## 🐛 Reporting Bugs

Open an issue using the **Bug Report** template. Include:
- OS and Python version
- Steps to reproduce
- Expected vs actual behavior
- Relevant log output

## 💡 Suggesting Features

Open an issue using the **Feature Request** template. Describe:
- The problem you're solving
- Your proposed solution
- Any alternatives you considered

## 🤝 Code of Conduct

Be welcoming, respectful, and constructive. We're all here to build something great together.

---

*Every breath counts — so does every line of code.* 🌿
