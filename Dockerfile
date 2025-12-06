# 1. Base Python image
FROM python:3.11-slim

# 2. Set working directory inside container
WORKDIR /app

# 3. Copy only code and requirements (not data)
COPY requirements.txt .
COPY scripts/ ./scripts/

# 4. Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Default command to keep container alive (interactive bash)
CMD ["bash"]
