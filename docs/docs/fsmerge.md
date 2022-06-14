Команда для слияния данных между ветками.

#### Синтаксис

```
| fsmerge 
  model=modelname
  outbranch=outbranchname
  inbranch=inbranchname
  [outbranchversion=outbranchversionnum]
  [inbranchverson=inbranchversionnum]
  [format=parquet | orc | json | csv]
```

#### Параметры

_Обязательные параметры_:

- **model** - название модели. 

- **outbranch** - название ветки, из которой считываются данные.

- **inbranch** - название ветки, в которую записываются данные.

Опциональные параметры:

- **outbranchversion** - номер версии ветки, из которой считываются данные.

- **inbranchversion** - номер версии ветки, в которую записываются данные.

- **format** - формат файлов, в котором они будут считываться из одной ветки и записываться в другую. Допустимые значения: parquet, orc, json, csv. По
  умолчанию `format=parquet`.
#### Примеры запросов

```
| fsmerge model=somemodel outbranch=branch1 inbranch=branch2 outbranchversion=1 inbranchversion=1 format=parquet
```