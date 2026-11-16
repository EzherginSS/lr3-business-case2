#!/bin/bash
# Подготовка рабочей папки конвейера и локального файла .env перед первым запуском.
# Для варианта 12 токен Kaggle не нужен: данные скачиваются с сайта UCI самим DAG.
# Файл .env содержит секреты и внесен в .gitignore, в репозиторий он не попадает.
set -e
mkdir -p dags/data
chmod 777 dags/data

touch .env
chmod 600 .env
grep -q '^AIRFLOW_UID=' .env || echo "AIRFLOW_UID=$(id -u)" >> .env
if ! grep -q '^SUPERSET_SECRET_KEY=' .env; then
    echo "SUPERSET_SECRET_KEY=$(openssl rand -base64 42 | tr -d '\n')" >> .env
fi

echo "Готово: создана dags/data, в .env записаны AIRFLOW_UID и случайный SUPERSET_SECRET_KEY."
echo "Если на ВМ нет доступа к archive.ics.uci.edu, положите файл 'Online Retail.xlsx' в dags/data вручную."
echo "Запуск окружения: docker compose up -d --build"
