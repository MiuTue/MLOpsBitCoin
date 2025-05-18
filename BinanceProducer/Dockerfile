FROM python:3.11
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/uv
ENV PATH="/uv/bin:${PATH}"
WORKDIR /src
COPY . .
RUN uv pip install --system -r requirements.txt

CMD [ "python", "BinanceProducer.py" ]
