# Start from a base image
FROM python:3.11-slim

# (Optional) Set a working directory
WORKDIR /app

# Copy requirements.txt and install the Python dependencies
COPY pyproject.toml .
COPY poetry.lock .
COPY README.md .
RUN pip3 install --no-cache-dir poetry
RUN poetry install --no-interaction --no-ansi --no-dev --no-root

# Copy the rest of the code
COPY plugin plugin
COPY main.py .
RUN poetry install --compile

# (Optional) Expose any ports your app uses
EXPOSE 7777

ENTRYPOINT ["poetry", "run", "main"]

# Specify the command to run when the container starts
CMD ["serve", "--address", "[::]:7777"]