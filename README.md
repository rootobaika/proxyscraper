# Proxy Scraper & Checker

Многопоточный парсер и чекер прокси-серверов. Собирает прокси из 100+ источников, проверяет на работоспособность, определяет страну по GeoIP и раскладывает по папкам `countries/` и `protocol/`.

## Возможности

- **100+ источников** — GitHub-репозитории, открытые API, HTML-страницы
- **HTTP/HTTPS, SOCKS4, SOCKS5**
- **Многопоточная проверка** — до 20 000 одновременных проверок
- **GeoIP** — определяет страну по IP (GeoLite2 MMDB)
- **Автоскачивание GeoIP-базы** при первом запуске
- **Фильтр прозрачных прокси** — не пропускает прокси, выдающие ваш реальный IP

## Структура вывода

```
result/{timestamp}/
├── http.txt              ← все собранные HTTP(S) прокси (до проверки)
├── socks4.txt
├── socks5.txt
├── all.txt
├── countries/            ← рабочие прокси по странам
│   ├── US/
│   │   ├── http.txt
│   │   ├── socks4.txt
│   │   ├── socks5.txt
│   │   └── all.txt
│   ├── DE/
│   ├── RU/
│   └── XX/               ← неизвестные страны
├── protocol/             ← рабочие прокси по протоколу
│   ├── http.txt
│   ├── socks4.txt
│   ├── socks5.txt
│   └── all.txt
└── result_counter.json   ← JSON со всеми результатами (отсортирован по пингу)
```

## Запуск

```bash
go build -o proxy_scraper .
./proxy_scraper
# или сразу
go run .
```

## GitHub Actions

Репозиторий настроен на автоматическую сборку под Linux и Windows при пуше в `main`/`master` и при создании тега `v*`. При создании тега автоматически создаётся Release с прикреплёнными бинарниками.

## Источники

Прокси собираются из:
- GitHub-репозиториев (TheSpeedX, monosans, ShiftyTR, rdavydov, и десятки других)
- API proxyscrape.com, openproxylist.xyz, proxyscan.io, proxy-list.download
- proxydb.net (HTML-парсинг)

## Требования

- Go 1.21+
- Для проверки прокси: доступ к httpbin.org (или другому серверу, указанному в `checkURL`)
- Для GeoIP: автоматически скачивается при запуске
