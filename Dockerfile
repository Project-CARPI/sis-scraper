FROM python:3.12-slim

ARG WORKDIR="scraper"
ARG CRON_JOB_PATH="cron_job.sh"
# Every day at 12:00 AM
ARG CRON_SCHEDULE="0 0 * * *"

# Install cron, curl, and git
RUN apt-get update && apt-get install -y cron curl git

# Clean up apt cache to reduce image size
RUN rm -rf /var/lib/apt/lists/*

# Store all application files in the specified working directory
WORKDIR /${WORKDIR}

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Set up cron job to run the specified script at the defined schedule
RUN echo "${CRON_SCHEDULE} root /${WORKDIR}/${CRON_JOB_PATH} >> /proc/1/fd/1 2>&1" > \
    /etc/cron.d/scraper-cron

# Grant appropriate permissions to the cron file and register it with cron
RUN chmod 0644 /etc/cron.d/scraper-cron && \
    crontab /etc/cron.d/scraper-cron

# Ensure the cron job bash script is executable
RUN chmod +x /${WORKDIR}/${CRON_JOB_PATH}

# Run cron in the foreground to capture logs in stdout
CMD ["cron", "-f"]