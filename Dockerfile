FROM apache/airflow:2.10.4

COPY pyproject.toml /opt/pyproject.toml
COPY requirements.txt /opt/requirements.txt
COPY config.cfg /opt/config.cfg

USER root

# Print the current working directory (for debugging)
RUN echo ${PWD}

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create and activate a Python virtual environment
ENV ENV_DIR=/opt/venv
RUN python -m venv ${ENV_DIR}
ENV PATH="/opt/venv/bin:$PATH"

# Print the current PATH 
RUN echo ${PATH}

# Switch back to airflow user
USER airflow

# # Install Python dependencies
# RUN pip install --no-cache-dir --upgrade pip \
#     && pip install --user pyodbc 
# RUN pip install --user --no-cache-dir -r /opt/requirements.txt
