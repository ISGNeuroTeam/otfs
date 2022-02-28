Команда для чтения данных из хранилища признаков (Feature Store).

#### Синтаксис

```
| fsget 
  [format=parquet | orc | json | csv] 
path=/path/read/from
```

#### Параметры

_Обязательные параметры_:

- **path** - путь к папке с файлами. Это
  относительный путь от родительской папки, которая указывается в storage.path файла plugin.conf.

Опциональные параметры:

- **format** - формат файла(ов). Допустимые значения: parquet, orc, csv, json. По умолчанию `format=parquet`.

#### Примеры запросов

```
| fsget format=parquet path=demo_train
```