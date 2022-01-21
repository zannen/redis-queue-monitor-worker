FROM python:3.8-alpine
ENTRYPOINT ["gunicorn"]
CMD ["--config", "gunicorn_config.py", "app:create_app()"]
RUN addgroup -g 1000 runner && adduser -g Runner -D -G runner -u 1000 runner
USER runner
WORKDIR /home/runner
ENV FLASK_APP=server FLASK_ENV=development PATH="/home/runner/.local/bin:$PATH" PYTHONPATH=/home/runner
RUN python -m pip install --user --upgrade pip
ADD requirements.txt ./
RUN pip install --user -r requirements.txt
ADD /app ./app
