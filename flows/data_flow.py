import pandas as pd
import requests
import json
import os
import time
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.artifacts import create_markdown_artifact


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def load_csv(file_path: str):
    return pd.read_csv(file_path, delimiter=';')


@task(retries=5, retry_delay_seconds=10)
def fetch_data_from_api(row, column_name):
    time.sleep(30)
    value = row[column_name]
    api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={value}&apikey=demo"
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


@task
def process_data(api_response):
    return pd.json_normalize(api_response)


@task
def save_to_json(data, output_dir: str, file_name: str):
    logger = get_run_logger()
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{file_name}.json")
    if isinstance(data, pd.DataFrame):
        data = data.to_dict(orient='records')
    logger.info(f"Сохраняем результат в {file_path}")
    with open(file_path, "w") as f:
        json.dump(data, f)

    create_markdown_artifact(
        f"**Результаты сохранены для {file_name}**\n\n"
        f"Файл сохранён по пути: `{file_path}`."
    )


@task
def send_telegram_notification(message: str, chat_id: str, bot_token: str):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message
    }
    requests.post(url, data=payload)


@flow(name="Data Processing Flow")
def data_processing_flow(file_path: str, output_dir: str, column_name: str, chat_id: str, bot_token: str):
    logger = get_run_logger()
    logger.info("Запуск потока обработки данных")

    data = load_csv(file_path)
    logger.info(f"Загружено {len(data)} строк из файла {file_path}")
    create_markdown_artifact(f"Загружено {len(data)} строк из файла {file_path}")

    for _, row in data.iterrows():
        try:
            api_response = fetch_data_from_api(row, column_name)
            logger.info(f"Получен ответ от API для {row[column_name]}")

            processed_data = process_data(api_response)
            save_to_json(processed_data, output_dir, str(row[column_name]))
            logger.info(f"Данные для {row[column_name]} сохранены в {output_dir}")
        except Exception as e:
            logger.error(f"Ошибка при обработке данных для {row[column_name]}: {e}")
            raise

    send_telegram_notification("Процесс завершён.", chat_id, bot_token)
    logger.info("Уведомление отправлено в Telegram")
    create_markdown_artifact(f"Обработка данных {file_path} завершена.")


if __name__ == "__main__":
    data_processing_flow(
        file_path="/path/to/CSV.csv",
        output_dir="/path/to/Result",
        column_name="symbol",
        chat_id="chat_id",
        bot_token="token"
    )