FROM python:3.11-slim

ENV TZ=Asia/Tokyo \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Supercronic version pin — update here to upgrade.
ARG SUPERCRONIC_VERSION=0.2.29

RUN apt-get update && apt-get install -y --no-install-recommends tini curl && \
    rm -rf /var/lib/apt/lists/* && \
    # Detect host architecture so the correct supercronic binary is fetched.
    # dpkg --print-architecture returns e.g. "arm64", "amd64" — matching
    # the supercronic release naming convention on GitHub.
    ARCH="$(dpkg --print-architecture)" && \
    curl -fsSL \
        "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-${ARCH}" \
        -o /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENTRYPOINT ["/usr/bin/tini", "--"]
