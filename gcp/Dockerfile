FROM google/cloud-sdk:alpine

RUN apk --update add openjdk8-jre
RUN gcloud components install pubsub-emulator beta --quiet

VOLUME /opt/data

COPY start-pubsub.sh .

EXPOSE 8432

ENTRYPOINT ["./start-pubsub.sh"]