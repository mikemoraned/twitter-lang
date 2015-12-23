FROM java:8

WORKDIR /app
ADD target/universal/stage /app
RUN ["chown", "-R", "daemon:daemon", "."]

USER daemon

EXPOSE 4040

ENTRYPOINT ["bin/twitter-lang"]
CMD []

