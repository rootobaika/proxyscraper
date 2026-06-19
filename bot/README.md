# Proxy Bot — Discord + Telegram

Бот для получения свежих рабочих прокси (HTTP, SOCKS4, SOCKS5).

## Установка

```bash
cd bot
python -m venv venv
source venv/bin/activate   # или venv\Scripts\activate на Windows
pip install -r requirements.txt
```

## Настройка

```bash
export DISCORD_TOKEN="ваш_дискорд_токен"
export TELEGRAM_TOKEN="ваш_телеграм_токен"
```

Можно указать только один из токенов — бот запустит только того, для кого есть токен.

## Запуск

```bash
python proxy_bot.py
```

## Команды

### Telegram
- `/start` — справка
- `/proxies` — свежие прокси всех типов
- `/proxies http` — только HTTP
- `/proxies socks4` — только SOCKS4
- `/proxies socks5` — только SOCKS5
- `/stats` — статистика кэша
- `/refresh` — принудительное обновление

### Discord
- `!proxies` — свежие прокси всех типов
- `!proxies http` — только HTTP
- `!proxies socks4` — только SOCKS4
- `!proxies socks5` — только SOCKS5

## Как это работает

1. При старте и каждые 30 минут бот скачивает прокси из >30 источников
2. Проверяет их через httpbin.org (до 500 одновременно)
3. Кэширует рабочие и отдаёт по запросу
4. Сортировка по пингу (самые быстрые первые)
