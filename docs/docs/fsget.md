Команда для чтения данных из хранилища признаков (Feature Store).

#### Синтаксис

```
| fsget 
  model=modelname
  [branch=branchname]
  [version=versionnum]
  [format=parquet | orc | json | csv] 
```

#### Параметры

_Обязательные параметры_:

- **model** - название модели.

Опциональные параметры:

- **branch** -  Название ветки, из которой считываются данные. По умолчанию `branch=main`.

- **version** -  Номер версии, из которой считываются данные. По умолчанию значение версии равно номеру последней существующей в ветке версии.

- **format** - формат считываемых файлов. Допустимые значения: parquet, orc, json, csv. По
  умолчанию `format=parquet`.

#### Примеры запросов

```
| fsget model=somemodel branch=anybranch version=1 format=csv
```