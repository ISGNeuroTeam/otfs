Команда для вывода информации о родительской ветке определенной ветки в модели.

#### Синтаксис

```
| fsgetparentbranch 
  model=modelname
  branch=branchname
  [showdataexistsinfo=true | false]
  [showcreationdate=true | false]
  [showlastupdatedate=true | false]
  [showlastversionnum=true | false]
  [showversionslist=true | false]
```

#### Параметры

_Обязательные параметры_:

- **model** - название модели.

- **branch** - название ветки.

Опциональные параметры:

- **showdataexistsinfo** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о наличии/отсутствии данных в ветке. По
  умолчанию `showdataexistsinfo=false`.

- **showcreationdate** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о дате и времени создания ветки. По
  умолчанию `showcreationdate=false`.

- **showlastupdatedate** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о дате и времени последнего обновления данных в ветке. По
  умолчанию `showlastupdatedate=false`.

- **showlastversionnum** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о номере последней версии данных в ветке. По
  умолчанию `showlastversionnum=false`.

- **showversionslist** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о наличии/остутствии дочерних веток. По
  умолчанию `showversionslist=false`. 

#### Примеры запросов

```
| fsgetparentbranch model=somemodel branch=anybranch showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true showversionslist=true
```