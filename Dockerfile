FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared ./shared
COPY agents ./agents

ENV PORT=8088
EXPOSE 8088

CMD ["python", "-m", "agents.copilot.app"]
