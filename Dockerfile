FROM python:3.12-slim

# Install cron and curl
RUN apt-get update && apt-get install -y cron curl

# Clean up apt cache to reduce image size
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /scraper
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

# Add crontab file to the container cron.d directory
COPY crontab /etc/cron.d/scraper-cron

RUN chmod 0644 /etc/cron.d/scraper-cron && crontab /etc/cron.d/scraper-cron

# Ensure the bash script is executable
RUN chmod +x /scraper/cron_job.sh

# Run cron in the foreground to capture logs in stdout
CMD ["cron", "-f"]