# ./replica/Dockerfile
FROM python:3.11-slim

WORKDIR /usr/src/app

# Install curl for healthcheck + dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python deps from the image layer (not the bind mount)
# requirements.txt is copied from the build context (./replica/)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Source code comes from the bind-mounted volume at runtime,
# but we copy it here too so the image is self-contained for CI/CD
COPY . .

# --reload watches /usr/src/app for file changes → hot-reload on save
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
