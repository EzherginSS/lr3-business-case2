#!/bin/bash
# Полная очистка окружения после сдачи работы
set -e
docker compose down -v --rmi local --remove-orphans
rm -rf dags/data .env
echo "Контейнеры, тома и локальные образы проекта удалены."
