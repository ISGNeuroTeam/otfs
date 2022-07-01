Команда для слияния данных между ветками.

#### Синтаксис

```
| fsmerge 
  model=modelname
  outbranch=outbranchname
  inbranch=inbranchname
  [outbranchversion=outbranchversionnum]
  [inbranchverson=inbranchversionnum]
```

#### Параметры

_Обязательные параметры_:

- **model** - название модели. 

- **outbranch** - название ветки, из которой считываются данные.

- **inbranch** - название ветки, в которую записываются данные.

Опциональные параметры:

- **outbranchversion** - номер версии ветки, из которой считываются данные.

- **inbranchversion** - номер версии ветки, в которую записываются данные.

#### Примеры запросов

```
| fsmerge model=somemodel outbranch=branch1 inbranch=branch2 outbranchversion=1 inbranchversion=1
```