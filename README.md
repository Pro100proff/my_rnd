# Full-cycle prototype: Flink indexing + Spark maintenance

Прототип покрывает **полный цикл**:

1. **Flink Indexer Job** читает сырые события из Kafka (`raw-archive-events`).
2. Пишет небольшие ORC-файлы (~1 МБ) в HDFS: `/tmp/{index_id}/dt=YYYY-MM-DD/hr=HH/slot-*.orc`.
3. По завершению checkpoint Flink асинхронно отправляет событие в Kafka (`merge-events`).
4. **Spark ArchiveMaintenanceJob** читает `merge-events` и выполняет merge + rotation + deferred cleanup.

## Компоненты

- `com.platform.archive.flink.FlinkArchiveIndexerJob`
  - источник: Kafka;
  - sink: rolling ORC writer в HDFS;
  - размер файлов: `--target-file-size-bytes 1048576`;
  - async публикация checkpoint-событий в `merge-events` через Kafka producer внутри sink.
- `com.platform.archive.maintenance.ArchiveMaintenanceJob`
  - long-running poll loop;
  - merge, rotation и deferred deletes через Postgres-метаданные.
- `com.platform.archive.tools.RawEventGenerator`
  - генератор сырых событий в `raw-archive-events`.

## Как запускать проект

### 1) Требования

- Docker + Docker Compose plugin (`docker compose`)
- 8+ GB RAM (рекомендуется 12+ GB для локального стенда)
- Открытые порты: `5432`, `9092`, `9870`, `8020`, `7077`, `8080`, `8082`

### 2) Сборка и старт

```bash
docker compose up --build -d
```

Проверить статус:

```bash
docker compose ps
```

Смотреть логи важных сервисов:

```bash
docker compose logs -f flink-archive-indexer
docker compose logs -f archive-maintenance
docker compose logs -f kafka-raw-generator
```

### 3) Проверка, что пайплайн живой

1. В логах `kafka-raw-generator` должны идти сообщения `generated=...`.
2. В логах `flink-archive-indexer` должны появляться отправки `merge-event sent ...`.
3. В логах `archive-maintenance` должны появляться сообщения `merge done index=...`.
4. В HDFS должны появляться файлы в `/tmp/{index_id}/dt=.../hr=...`.

Проверка HDFS через UI: `http://localhost:9870`.

### 4) Остановка

```bash
docker compose down
```

С удалением томов (полный reset):

```bash
docker compose down -v
```

## Что поднимается в compose

- Postgres
- Kafka + Zookeeper
- HDFS (NameNode/DataNode)
- Spark master/worker + Spark maintenance job
- Flink JobManager/TaskManager + Flink indexer submitter
- `kafka-raw-generator` (непрерывно генерирует raw события в `raw-archive-events`)

## Полезные UI

- HDFS NameNode UI: `http://localhost:9870`
- Spark master UI: `http://localhost:8080`
- Flink UI: `http://localhost:8082`

## Важные детали

- Flink отправляет merge-event **асинхронно** в `notifyCheckpointComplete`.
- Ключ сообщения merge-event = `index_id`, чтобы события шли последовательно по индексу.
- Spark merge пропускает `.in-progress` и слишком свежие файлы, что защищает от чтения незавершённых файлов.

## Maven (локальная сборка jar)

```bash
mvn -DskipTests package
```
