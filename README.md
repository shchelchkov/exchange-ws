# exchange-ws

Сервис-мост **exchange-ws WebSocket -> Kafka**. Подключается к публичному WS-потоку exchange-ws,
парсит сообщения, обогащает их метаданными (setting_code, config_code, instant, date_time)
и публикует в Kafka. Стримами управляют через HTTP API; сервис регистрируется в Eureka.

```value

  "value_key": [
    "topic"
  ],
  "setting_code": "settings_code",
  "key": "key",
  "value": {
    "topic": "topic",
    "code": "code",
    "symbol": "symbol"
  }

```