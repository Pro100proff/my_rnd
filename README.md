# Archive Maintenance Spark Job (prototype)

Прототип long-running Spark Job, который читает события из Kafka (`merge-events`) и для каждого `index_id` последовательно выполняет:

1. `merge` мелких ORC файлов из `/tmp/...`.
2. `rotation` по правилам из Postgres.
3. `deferred cleanup` (каждый 10-й цикл).

## Основные классы

- `ArchiveMaintenanceJob` — основной цикл poll/group/process/commit.
- `MergeExecutor` — идемпотентный merge и удаление исходников.
- `RotationEvaluator` — строит список действий из `rotation_rules`.
- `RotationExecutor` — применяет drop/filter/sampling/switch.
- `DeferredCleanupService` — удаляет директории после `grace period`.
- `PostgresClient` — доступ к метаданным и служебным таблицам.

## Локальный запуск (docker compose)

```bash
docker compose up --build
```

Поднимаются: Postgres, Kafka, Spark master/worker и контейнер с job.

## Maven сборка

```bash
mvn -DskipTests package
```

Jar: `target/archive-maintenance.jar`

## Примечания по прототипу

- В проде подразумевается запуск на YARN (`spark-submit --master yarn --deploy-mode cluster`).
- В compose используется standalone Spark для демонстрации потока.
- `directory_switch` в прототипе создаёт новый путь как `activeDir-YYYY-MM-DD`.
- Атомарная перезапись партиций реализована через `.__rewrite_tmp` + rename.

## Минимальная доработка Flink

В текущий Flink job добавить отправку события в Kafka в `notifyCheckpointComplete(checkpointId)`:

```java
// pseudo-code
@Override
public void notifyCheckpointComplete(long checkpointId) {
    CheckpointEvent event = new CheckpointEvent(indexId, checkpointId, finalizedFiles, totalBytes, Instant.now());
    producer.send(new ProducerRecord<>("merge-events", indexId, toJson(event)));
}
```
