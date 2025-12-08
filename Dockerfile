# 1. Base Python image
FROM python:3.11-slim

# 2. Set working directory inside container
WORKDIR /app

# 3. Copy only code, requirements, and not data. Data will be mounted (pointed to local storage)
COPY requirements.txt .
COPY scripts/ ./scripts/
# COPY data/f1 ./data/f1

# 4. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Default command to keep container alive (interactive bash)
CMD ["bash"]
