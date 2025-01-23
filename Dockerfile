# Use Python 3.9-slim as the base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Install system dependencies (including PostgreSQL development libraries)
RUN apt-get update && apt-get install -y \
    wget \
    default-jdk \
    procps \
    libpq-dev \
    gcc \
    python3-dev \
    && mkdir -p /opt/jdbc \
    && wget -q -O /opt/jdbc/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
    && rm -rf /var/lib/apt/lists/*

# Copy the application code
COPY . .

# Upgrade pip first (Recommended)
RUN pip install --no-cache-dir --upgrade pip

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set JAVA_HOME and update PATH
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:$PATH"

# Expose Streamlit’s default port
EXPOSE 8501

# Default command: Run main.py and Streamlit in parallel
CMD ["bash", "-c", "python main.py & streamlit run app/app.py --server.port=8501 --server.address=0.0.0.0"]
