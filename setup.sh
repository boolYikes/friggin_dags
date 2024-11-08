#!/bin/bash

echo "USER_UID=$(id -u)" > .env && docker compose up
