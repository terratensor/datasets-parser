# datasets-parser

Обрабатывает csv data файлы в папке data и записывает их в базу данных

При обработке пропускает архивы и иные файлы, которы не csv 

Поэтому при необходимости надо распаковать архивы перед тем как запускать обработку csv файлов:

- globalterrorismdb_full_may2023.7z
- world-postal-code.csv

Если учитывать распакованные вышеуказанные архивы, то после обработки в БД будет 2 164 199 записей.

### Системные требования

Для работы необходимы docker и git 

В docker контейнере будет создана и запущена база данных postgres

Git нужен для того, чтобы скачать репозиторий с проектом.

Так же можно воспользоваться прямой ссылкой для скачивания проекта https://github.com/terratensor/datasets-parser/archive/refs/heads/main.zip 

### Как обработать файлы и получить БД

Создаем папку на диске для проектов:
```
mkdir terratensor
```
Выбираем созданную папку
```
cd terratensor
```
Скачиваем репозиторий 
```
git clone https://github.com/terratensor/datasets-parser.git
```
Запускаем докер контейнер с базой данных
```
docker compose up -d
```
Скачиваем последнюю версию парсера

https://github.com/terratensor/datasets-parser/releases/latest

Сохраняем в папку с проектом, запускаем

```
./datasets-parser.exe -d ./data
```

`-d ./data` — путь до папки в которой хранятся csv файлы для обработки
Если вы сохраните утилиту datasets-parser.exe в корень проекта, то достаточно запустить exe файл без указания дополнительных параметров.

При каждом новом запуске база не удаляется, а пополняется снова.
Так что будьте внимательны, обычно процедура обработки файлов достаточно запустить один раз.

Для просмотра БД необходима программа для работы с базами данных, рекомендуем [DBeaver Community](https://dbeaver.io/download/)

Установите программу, запустите. Создайте новое соединение с БД, клавиши `ctrl+shift+n`

<details><summary>Details</summary>
<p>

  ![2024-05-02_10-17-28](https://github.com/terratensor/datasets-parser/assets/10896447/41136351-ddbb-467a-88ab-1ccdc966363d)

</p>
</details> 


Выберите тип нового соединения PostgreSQL

<details><summary>Details</summary>
<p>

![2024-05-02_10-19-18](https://github.com/terratensor/datasets-parser/assets/10896447/e2e93e19-a26c-4439-aea9-e9d1ad2aa0e2)

</p>
</details> 


Введите данные для соединения:
- База данных: geomatrix
- Прользователь: app
- Пароль: secret
- Хост: localhost
- Порт: 54325

<details><summary>Details</summary>
<p>

![2024-05-02_10-22-24](https://github.com/terratensor/datasets-parser/assets/10896447/072994b1-b049-4738-963e-52877cfd7e87)

</p>
</details> 


Справа в списке баз данных появится наименование geomatrix, разверните до таблицы: db_entities, как показано на рисунке, переключитель на вкладку данные
<details><summary>Details</summary>
<p>

![2024-05-02_10-26-23](https://github.com/terratensor/datasets-parser/assets/10896447/daec57e1-17ae-473a-92ca-81649b9d0cc5)

</p>
</details> 

