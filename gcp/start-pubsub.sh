#!/usr/bin/env bash

gcloud config set project ${PUBSUB_PROJECT_ID}

gcloud beta emulators pubsub start \
  --data-dir=/opt/data \
  --host-port=${PUBSUB_LISTEN_ADDRESS}