# Use the official Ubuntu base image
FROM ubuntu:latest

# Update the package repository and install any basic tools (optional but recommended)
RUN apt-get update && apt-get install -y \
    curl \
    vim \
    7zip \
    btop \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"
RUN uv tool install csvpeek
RUN curl -O https://excelbianalytics.com/wp/wp-content/uploads/2017/07/10000-Sales-Records.zip
RUN 7z e 10000-Sales-Records.zip -o/app/sales.csv

# Set the working directory inside the container
WORKDIR /app

# Set the default command to run Bash
CMD ["/bin/bash"]

