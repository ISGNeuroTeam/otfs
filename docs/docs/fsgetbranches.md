Команда для вывода информации о всех ветках в модели.

#### Синтаксис

```
| fsgetbranches 
  model=modelname
  [showdataexistsinfo=true | false]
  [showcreationdate=true | false]
  [showlastupdatedate=true | false]
  [showlastversionnum=true | false]
  [haschildbranches=true | false]
  [showversionslist=true | false]
  [onlyempty=true | false]
  [onlynonempty=true | false]
  [onlywithchildbranches=true | false]
  [onlywithoutchildbranches=true | false]
```

#### Параметры

_Обязательные параметры_:

- **model** - название модели.

Опциональные параметры:

- **showdataexistsinfo** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о наличии/отсутствии данных в ветке. По
  умолчанию `showdataexistsinfo=false`.

- **showcreationdate** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о дате и времени создания ветки. По
  умолчанию `showcreationdate=false`.

- **showlastupdatedate** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о дате и времени последнего обновления данных в ветке. По
  умолчанию `showlastupdatedate=false`.

- **showlastversionnum** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о номере последней версии данных в ветке. По
  умолчанию `showlastversionnum=false`.

- **haschildbranches** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о наличии/остутствии дочерних веток. По
  умолчанию `haschildbranches=false`.

- **showversionslist** - указывает, будет ли в результирующей таблице выводиться колонка, содержащая информацию о наличии/остутствии дочерних веток. По
  умолчанию `showversionslist=false`.

- **onlyempty** - указывает, будет ли в результирующей таблице выводиться информация только по веткам, не содержащим данные. По
  умолчанию `onlyempty=false`.

- **onlynonempty** - указывает, будет ли в результирующей таблице выводиться информация только по веткам, содержащим данные. По
  умолчанию `onlynonempty=false`.

- **onlywithchildbranches** - указывает, будет ли в результирующей таблице выводиться информация только по веткам, имеющим дочерние ветки. По
  умолчанию `onlywithchildbranches=false`.

- **onlywithoutchildbranches** - указывает, будет ли в результирующей таблице выводиться информация только по веткам, не имеющим дочерние ветки. По
  умолчанию `onlywithoutchildbranches=false`.

#### Примечание 

     Невозможно указать в одном запросе параметры onlyempty и onlynonempty или onlywithchildbranches и onlywithoutchildbranches со значением true - это вызовет ошибку.    

#### Примеры запросов

```
| fsgetbranches model=somemodel showdataexistsinfo=true showcreationdate=true showlastupdatedate=true showlastversionnum=true haschildbranches=true showversionslist=true onlynonempty=true onlywithchildbranches=true  
```