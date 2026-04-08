FROM python:3.12-slim

LABEL maintainer="student"

RUN mkdir /app

COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8090

ENTRYPOINT ["python"]
CMD ["app.py"]