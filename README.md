# de-project-sprint-2-2023

Привет, ревьювер :) 
Я работаю продуктовым аналитиком 4 года. Сейчас учусь на дата инженера (ну ооочень надо научиться оптимизации, чтобы меня не проклинали другие юзеры БД) 

Я немного переделала код из шаблона, и буду рада дискуссии на тему «какой синтаксис лучше юзать»

## Этап 1. Добавление нового источника

###1.1 Преданалитика: проверка данных на аномалии и пропуски.
Проверила типы данных – они соответствуют типам целевой таблицы.

Проверила, нет ли пропусков:
```sql 
SELECT * FROM external_source.craft_products_orders
WHERE  craftsman_id isnull 	
	OR craftsman_birthday isnull 
	OR craftsman_address isnull 
	OR craftsman_email isnull 
	OR product_id isnull 
	OR product_price isnull 
	OR product_name isnull
	OR order_сreated_date isnull 
	OR order_status isnull 
	OR customer_id isnull 
```
(их нет)

Проверила, что нет дублирующихся order_id:
```sql
SELECT order_id, count(1) 
FROM external_source.craft_products_orders
GROUP BY 1
HAVING count(1) > 1
```
(тоже всё идеально)


На всякий случай проверила, нет ли дублирующихся заказов по связке customer_id + product_id. Ничего нет. И, что удивительно – у каждого customer_id есть только один заказ, что говорит об отсутствии повторных покупок или ретеншена. Но, возможно, платформа просто новая платформа недавно начала свою деятельность и обработала немного заказов. 


###1.2 Для выполнения первого задания – добавления нового источника – достаточно добавить в уже существующий скрипт `UNION`ом новые данные. 

Далее `MERGE` автоматически распределит данные из нового источника в соответствующие измерения в схеме dwh: d_craftsman, d_customer, d_product, f_order. DDL с добавленным UNION'ом положила в папку `scripts`.

Проверила обновлённый `tmp_source` на дубликаты и мне не понравилось, что на выходе получаем под одним `order_id` несколько разных заказов: 4 источника – 4 заказа под одним и тем же идентификатором. Но в DDL `f_order` генерируется уникальный идентификатор: `GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE)` что избавляет нас от проблемы с дублирующимися order_id, это поле в общем-то и не участвует в MERGE. 

Но проверка на уникальные `order_status` 
```sql
SELECT order_status, count(1) 
FROM dwh.f_order  
GROUP BY 1
```
показывает, что у нас есть два статуса, показывающих, что заказ в работе: in-progress (1 строка), in progress (1029 строк). Я в работе стараюсь избегать пробелов, поэтому заменю всё на in_progress. Условие добавлено в DDL_tmp_source_update.sql:

```sql
   UPDATE dwh.f_order
SET order_status = 'in_progress'
WHERE order_status = 'in-progress' OR order_status = 'in progress';
```

## Этап 2. Сбор витрины

### 2.1 Собираю витрину `dwh.customer_report_datamart` 

Сначала создадим таблицу, укажем типы данных и напишем комментарии для BI аналитиков, которые будут использовать эти данные. DDL_customer_report_datamart_create в папке со скриптами.

### 2.2 Смотрю на «скелет» из урока 

Его структура:

```sql
WITH
dwh_delta AS (
    -- Шаг 2
),
dwh_update_delta AS (
    -- Шаг 3
),
dwh_delta_insert_result AS (
    -- Шаг 4
),
dwh_delta_update_result AS (
    -- Шаг 5
),
insert_delta AS (
    -- Шаг 6
),
update_delta AS (
    -- Шаг 7
),
insert_load_date AS (
    -- Шаг 8
)
SELECT 'increment datamart'; 
```

Не совсем понимаю, для чего dwh_update_delta (шаг 3). Его использование выглядит избыточным, так как он просто отбирает существующих кастомеров. Можно просто использовать условие `WHERE exist_customer_id NOTNULL` в `dwh_delta_update_result`.

Создаю DDL таблицы для инкрементальных загрузок `dwh.load_dates_customer_report_datamart`

Добавляю СТЕ для расчёта самой популярной категории за отчётный период и самого популярного мастера в разрезе заказчика. Вывести их в отдельные СТЕ гораздо читабельнее, чем добавлять громоздкую матрёшку из вложенных запросов – она просто взрывает мозг.

```sql
 top_product AS
    (SELECT 
           customer_id,
           report_period,
           product_type,
            RANK() OVER (PARTITION BY customer_id, report_period ORDER BY COUNT(product_id) DESC) AS ranked_product
      FROM dwh_delta
      GROUP BY customer_id, report_period, product_type),
            
 top_craftsmen AS (
      SELECT 
           customer_id,
           craftsman_id,
           RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(craftsman_id) DESC) AS ranked_craftsman
      FROM dwh_delta
      GROUP BY customer_id, craftsman_id),
```

В СТЕ `dwh_delta_insert_result` в джойне добавляю условие присоединения: 
```sql
 INNER JOIN top_product tp ON tp.customer_id = t1.customer_id 
    	AND tp.report_period = t1.report_period
    	AND ranked_product = 1
    
    INNER JOIN top_craftsmen tc ON tc.customer_id = t1.customer_id
    	AND ranked_craftsman = 1
```

Оптимизирую кусок с подсчётом количества заказов в разных статусах:
```sql
        COUNT(order_id) FILTER (WHERE t1.order_status = 'created') AS count_order_created,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'in_progress') AS count_order_in_progress,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'delivery') AS count_order_delivery,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'done') AS count_order_done,
        COUNT(order_id) FILTER (WHERE t1.order_status <> 'done') AS count_order_not_done 

```

Такой вариант поддерживается только PostgreSQL и отрабатывает быстрее, чем конструкция с CASE, которая сначала оценивает условие, и только затем суммирует. Тренажёр в Яндекс Практикуме не поддерживает такой вариант: преподаватель объясняет, что синтаксис, который используется только в постгресе, не перенести на спарк или другие аналитические базы.

Скрипт в рамках проекта пока не планируется переносить на спарк или куда-либо ещё, поэтому оставлю такой вариант.

Кстати, тренажёр поддерживает соединение таблиц с помощью оператора USING :) а ведь это фишка Постгреса

Добавляем фильтр, чтобы оставить только несуществующих кастомеров и идём дальше.

###2.3 Этап `dwh_delta_update_result` 

Блок почти полностью копирует предыдущий, за исключением фильтрации: нам нужны только существующие покупатели.

###2.4 Финиш

Обновляем, снова обновляем, добавляем дату загрузки и коммитим.

