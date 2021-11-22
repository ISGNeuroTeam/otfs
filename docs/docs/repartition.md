Команда для репартиционирования текущего датафрейма. Обычно имеет смысл применять, если исходный датафрейм состоит 
из большого количества партиций (например, он появился в результате чтения большого количества папок).

#### Синтаксис

```
| repartition num=<int>
```

#### Параметры

_Обязательные параметры_:

- **num** - количество партиций, на которые надо разбить датафрейм. Значение должно быть целым.

#### Примеры запросов

```
| repartition num=16 
```