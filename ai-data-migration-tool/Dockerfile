# Shared multi-user demo of the AI Data Migration Tool.
#   docker build -t amt-dashboard .
#   docker run -p 8765:8765 amt-dashboard
FROM python:3.12-slim

WORKDIR /app
RUN pip install --no-cache-dir "pyarrow>=14"
COPY amt ./amt
COPY eval ./eval
COPY results ./results

ENV AMT_UI_WORKDIR=/tmp/amt_ui \
    PYTHONUNBUFFERED=1
EXPOSE 8765

# Public mode: built-in demo databases and private downloads only.
CMD ["python", "-m", "amt", "ui", "--host", "0.0.0.0", "--port", "8765", "--public", "--no-browser"]
