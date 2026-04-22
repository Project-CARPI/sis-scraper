FROM python:3.12-slim

# Install cron, curl, and git
RUN apt-get update && apt-get install -y cron curl git

# Clean up apt cache to reduce image size
RUN rm -rf /var/lib/apt/lists/*

# Store all application files in /scraper
WORKDIR /scraper

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code into the container
COPY . .

# Add crontab file to the container cron.d directory
COPY crontab /etc/cron.d/scraper-cron

# Give execution rights on the cron job and install the cron job
RUN chmod 0644 /etc/cron.d/scraper-cron
RUN crontab /etc/cron.d/scraper-cron

# Ensure the bash script is executable
RUN chmod +x /scraper/cron_job.sh

# Run cron in the foreground to capture logs in stdout
CMD ["cron", "-f"]