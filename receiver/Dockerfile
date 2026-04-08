# starts from a python image
FROM python:3.12-slim

LABEL maintainer="student"

RUN mkdir /app

# copy dependencies
COPY requirements.txt /app/requirements.txt

WORKDIR /app

# docker installs packages
RUN pip install --no-cache-dir -r requirements.txt

# copy application code
COPY . /app

RUN chown -R nobody:nogroup /app
USER nobody

EXPOSE 8080

#start service
ENTRYPOINT ["python"]
CMD ["app.py"]