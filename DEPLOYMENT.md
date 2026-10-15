# 🚀 GitHub Deployment & Production Operations Guide

This guide provides step-by-step instructions for:
1. Publishing **PulseQueue** to GitHub.
2. Setting up automated CI/CD and PyPI releases.
3. Deploying PulseQueue workers in production (Docker Compose, Systemd, Kubernetes).

---

## Part 1: Publishing PulseQueue to GitHub

### Step 1: Initialize Git and Verify Status
Make sure all your local files are committed:
```bash
git status
```

If you need to switch the default branch to `main`:
```bash
git branch -M main
```

### Step 2: Create a New GitHub Repository

#### Method A: Using GitHub CLI (`gh`)
If you have the GitHub CLI installed and authenticated:
```bash
gh repo create pulsequeue --public --source=. --remote=origin --push
```

#### Method B: Using GitHub Web UI
1. Go to [github.com/new](https://github.com/new).
2. Name the repository: `pulsequeue` (or `your-username/pulsequeue`).
3. Set visibility to **Public**.
4. Leave **"Initialize this repository with a README" unchecked** (since you already have one).
5. Click **Create repository**.
6. Copy the commands under *"push an existing repository from the command line"*:
   ```bash
   git remote add origin https://github.com/<your-username>/pulsequeue.git
   git push -u origin main
   ```

### Step 3: Verify GitHub Actions CI
Once pushed, click the **Actions** tab on your GitHub repository. You will see the **CI / Quality Assurance** pipeline automatically running:
* **Lint & Type Check:** Runs `ruff check .`, `ruff format --check .`, and `mypy src`.
* **Test Suite Matrix:** Runs unit and integration tests across **Python 3.10, 3.11, 3.12, and 3.13** against a live Redis service container.

---

## Part 2: Production Deployment Strategies

### Strategy A: Docker Compose (Single or Multi-Node VM)

The included `docker-compose.yml` provides a production-ready blueprint with healthchecks and automatic restarts:

```bash
# 1. Clone your repository onto your production server (AWS EC2, DigitalOcean droplet, etc.)
git clone https://github.com/<your-username>/pulsequeue.git
cd pulsequeue

# 2. Build and launch Redis and 2 worker nodes in background
docker compose up -d --build

# 3. Check live worker logs
docker compose logs -f worker-1 worker-2

# 4. Scale worker instances on demand
docker compose up -d --scale worker-1=4
```

---

### Strategy B: Systemd Service (Bare Metal / Ubuntu Linux)

If deploying directly onto Linux virtual machines without Docker:

1. Install Python 3.11 and create a virtual environment in `/opt/pulsequeue`:
   ```bash
   sudo mkdir -p /opt/pulsequeue
   sudo chown -R $USER:$USER /opt/pulsequeue
   cd /opt/pulsequeue
   python3 -m venv .venv
   .venv/bin/pip install --upgrade pip
   .venv/bin/pip install pulsequeue
   ```

2. Create a Systemd service file at `/etc/systemd/system/pulsequeue-worker.service`:
   ```ini
   [Unit]
   Description=PulseQueue Distributed Worker Node
   After=network.target redis.target

   [Service]
   Type=simple
   User=www-data
   Group=www-data
   WorkingDirectory=/opt/pulsequeue
   ExecStart=/opt/pulsequeue/.venv/bin/pulsequeue worker \
       --broker redis://127.0.0.1:6379/0 \
       --queue default \
       --concurrency 16
   Restart=always
   RestartSec=5
   KillSignal=SIGTERM
   TimeoutStopSec=30

   [Install]
   WantedBy=multi-user.target
   ```

3. Enable and start the service:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable pulsequeue-worker
   sudo systemctl start pulsequeue-worker
   sudo systemctl status pulsequeue-worker
   ```

---

### Strategy C: Kubernetes / Cloud Native Deployment

For Kubernetes clusters, deploy workers as a `Deployment` with HPA (Horizontal Pod Autoscaler):

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: pulsequeue-worker
  labels:
    app: pulsequeue
spec:
  replicas: 3
  selector:
    matchLabels:
      app: pulsequeue-worker
  template:
    metadata:
      labels:
        app: pulsequeue-worker
    spec:
      containers:
      - name: worker
        image: ghcr.io/<your-username>/pulsequeue:latest
        args: ["worker", "--broker", "redis://redis-service.default.svc.cluster.local:6379/0", "--concurrency", "10"]
        resources:
          limits:
            cpu: "1000m"
            memory: "512Mi"
          requests:
            cpu: "250m"
            memory: "128Mi"
        env:
        - name: PYTHONUNBUFFERED
          value: "1"
```

---

## Part 3: Automated PyPI Publishing (Optional)

To publish your package to PyPI on every new Git release tag:

1. Create an account on [PyPI](https://pypi.org/).
2. Create a **Trusted Publisher** under PyPI Account Settings pointing to your GitHub repository (`<your-username>/pulsequeue`).
3. Add `.github/workflows/release.yml`:
   ```yaml
   name: Release to PyPI
   on:
     release:
       types: [published]
   jobs:
     pypi-publish:
       name: Build & Publish
       runs-on: ubuntu-latest
       permissions:
         id-token: write
       steps:
         - uses: actions/checkout@v4
         - uses: actions/setup-python@v5
           with:
             python-version: "3.11"
         - run: pip install build && python -m build
         - uses: pypa/gh-action-pypi-publish@release/v1
   ```
4. Whenever you create a new GitHub release (e.g. `v0.1.0`), GitHub Actions will automatically build and publish the wheel to PyPI!
