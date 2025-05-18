FROM public.ecr.aws/bitnami/spark:3.5
USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uv/bin/uv
ENV PATH="/uv/bin:${PATH}"
WORKDIR /src
COPY . .
RUN uv pip install --system -r requirements.txt
RUN curl https://repo1.maven.org/maven2/org/apache/ivy/ivy/2.5.2/ivy-2.5.2.jar -o ivy.jar
RUN java -jar ivy.jar -ivy ivy.xml -retrieve "/opt/bitnami/spark/jars/[conf]-[artifact]-[type]-[revision].[ext]"

# Run the consumer application instead of Spark master
CMD ["python", "BinanceConsumer.py"]
