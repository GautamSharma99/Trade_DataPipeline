FROM python:3.11-slim-bookworm
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Charting library (TradingView Lightweight Charts, Apache-2.0), vendored so the demo works offline
RUN mkdir -p static/vendor && python -c "import urllib.request as u; u.urlretrieve(\
'https://unpkg.com/lightweight-charts@5.0.9/dist/lightweight-charts.standalone.production.js', \
'static/vendor/lightweight-charts.js')"

COPY main.py .
COPY static/ static/
ENV PYTHONUNBUFFERED=1 TZ=UTC
EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
