Команда для чтения данных из хранилища признаков (Feature Store).

#### Синтаксис

```
| fsget 
  [format=parquet | csv | orc] 
path=/path/read/from
```

#### Параметры

_Обязательные параметры_:

- **path** - путь к папке с файлами (при форматах parquet, orc) или к конкретному файлу (при форматах csv, json). Это 
  относительный путь от родительской папки, которая указывается в блоке storage файла plugin.conf.

Опциональные параметры:

- **format** - формат файла(ов). Допустимые значения: parquet, orc, csv, json. По умолчанию `format=parquet`.

#### Примеры запросов

```
| fsget format=parquet path=mechfond/well_daily_params 
```