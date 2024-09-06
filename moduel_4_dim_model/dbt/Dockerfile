# Use a lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /usr/app

# Install dbt and dbt-postgres adapter
RUN pip install --upgrade pip \
    && pip install dbt-core dbt-postgres

# Copy the local project files to the container
COPY . .

# Set the environment variable for dbt profiles.yml location
ENV DBT_PROFILES_DIR=/usr/app/.dbt

# Default command to run dbt
CMD ["dbt", "run"]
