FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY data/ ./data/
COPY START_APEX_V7.bat .
COPY README.md .

# Create .env template (user needs to add their own)
RUN echo "KITE_API_KEY=your_api_key\nKITE_ACCESS_TOKEN=your_access_token" > .env.example

# Expose port
EXPOSE 5000

# Run the application
CMD ["python", "backend/app.py"]
