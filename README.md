# Full-cycle prototype: Flink indexing + Spark maintenance

Прототип теперь покрывает **полный цикл**:

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

## Docker Compose

```bash
docker compose up --build
```

Поднимаются:
- Postgres
- Kafka + Zookeeper
- HDFS (NameNode/DataNode)
- Spark master/worker + Spark maintenance job
- Flink JobManager/TaskManager + Flink indexer submitter
- seed producer (кладёт тестовые raw события в `raw-archive-events`)

### Полезные UI

- HDFS NameNode UI: `http://localhost:9870`
- Spark master UI: `http://localhost:8080`
- Flink UI: `http://localhost:8082`

## Важные детали

- Flink отправляет merge-event **асинхронно** в `notifyCheckpointComplete`.
- Ключ сообщения merge-event = `index_id`, чтобы события шли последовательно по индексу.
- Spark merge пропускает `.in-progress` и слишком свежие файлы, что защищает от чтения незавершённых файлов.

## Maven

```bash
mvn -DskipTests package
```
