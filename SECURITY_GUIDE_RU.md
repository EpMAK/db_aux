# Безопасность БД: SQL-инъекции, права PostgreSQL, аутентификация MongoDB

## 1. SQL-инъекция в Python и защита через параметризацию

### Уязвимый пример (`psycopg2`)

```python
import psycopg2

conn = psycopg2.connect("dbname=testdb user=app_user password=secret host=127.0.0.1")
cur = conn.cursor()

email = input("email: ")
password = input("password: ")

# УЯЗВИМО: подстановка пользовательского ввода в SQL-строку
query = f"SELECT id FROM users WHERE email = '{email}' AND password = '{password}'"
cur.execute(query)

print(cur.fetchall())
cur.close()
conn.close()
```

Почему это опасно:
- пользователь может передать SQL-фрагмент вместо обычного текста;
- пример вредоносного ввода: `email = ' OR 1=1 --`;
- итог: проверка логина/пароля обходится.

### Безопасный пример (`psycopg2`)

```python
import psycopg2

conn = psycopg2.connect("dbname=testdb user=app_user password=secret host=127.0.0.1")
cur = conn.cursor()

email = input("email: ")
password = input("password: ")

# БЕЗОПАСНО: параметры передаются отдельно от SQL-шаблона
cur.execute(
    "SELECT id FROM users WHERE email = %s AND password = %s",
    (email, password),
)

print(cur.fetchall())
cur.close()
conn.close()
```

Почему это безопаснее:
- SQL и данные разделены;
- драйвер экранирует и передает значения как параметры, а не как часть SQL-кода.

### Безопасный пример (`SQLAlchemy`)

```python
from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://app_user:secret@127.0.0.1/testdb")

email = input("email: ")
password = input("password: ")

with engine.connect() as conn:
    rows = conn.execute(
        text("SELECT id FROM users WHERE email = :email AND password = :password"),
        {"email": email, "password": password},
    ).fetchall()

print(rows)
```

Практический вывод:
- запрещено формировать SQL через f-string, `%`-форматирование, конкатенацию;
- используйте только параметризованные запросы и ORM/Core API.

## 2. PostgreSQL: запрет пользователю удалять таблицы

Цель: пользователь приложения (`app_user`) может читать/писать данные, но не может удалить таблицу (`DROP TABLE`).

Ключевой принцип:
- `app_user` не должен быть владельцем таблиц.

### SQL-настройка ролей и прав

```sql
-- Выполнять под администратором PostgreSQL
CREATE ROLE app_owner NOLOGIN;
CREATE ROLE app_user LOGIN PASSWORD 'strong_password';

GRANT CONNECT ON DATABASE testdb TO app_user;
GRANT USAGE ON SCHEMA public TO app_user;

-- Запретить создание объектов в public
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM app_user;

-- Разрешить только DML-операции с таблицами
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app_user;

-- Права по умолчанию для новых таблиц, создаваемых app_owner
ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;

ALTER DEFAULT PRIVILEGES FOR ROLE app_owner IN SCHEMA public
GRANT USAGE, SELECT ON SEQUENCES TO app_user;
```

Если существующие таблицы принадлежат `app_user`, передайте владение:

```sql
ALTER TABLE public.users OWNER TO app_owner;
```

Проверка результата:
- под `app_user` выполните `DROP TABLE public.users;`;
- ожидается ошибка доступа (нет прав/владения).

## 3. MongoDB: базовая аутентификация

### Включение авторизации

В `mongod.conf` включите:

```yaml
security:
  authorization: enabled
```

### Создание администратора

До включения авторизации (или через localhost exception) создайте администратора:

```javascript
use admin
db.createUser({
  user: "root_admin",
  pwd: "strong_root_password",
  roles: [{ role: "root", db: "admin" }]
})
```

После этого перезапустите MongoDB.

### Создание прикладного пользователя

```javascript
use appdb
db.createUser({
  user: "app_user",
  pwd: "strong_app_password",
  roles: [{ role: "readWrite", db: "appdb" }]
})
```

Пример подключения:

```bash
mongosh "mongodb://app_user:strong_app_password@127.0.0.1:27017/appdb?authSource=appdb"
```
