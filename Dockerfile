FROM python:3.13-slim

WORKDIR /app

RUN pip install uv

COPY pyproject.toml uv.lock ./
RUN uv sync --system --frozen --no-dev

COPY . .

ENV PYTHONPATH=/app

CMD ["bash"]
