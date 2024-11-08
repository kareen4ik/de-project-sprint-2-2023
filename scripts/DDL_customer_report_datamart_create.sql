
DROP TABLE IF EXISTS dwh.customer_report_datamart;
CREATE TABLE IF NOT EXISTS dwh.customer_report_datamart (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL, 
    customer_name VARCHAR NOT NULL,
    customer_address VARCHAR NOT NULL,
    customer_birthday DATE NOT NULL, 
    customer_email VARCHAR NOT NULL, 
    total_spent NUMERIC(15,2) NOT NULL, 
    platform_earnings NUMERIC(15,2) NOT NULL, 
    count_order BIGINT NOT NULL, 
    avg_order_price NUMERIC(10,2) NOT NULL, 
    median_time_order_completed NUMERIC(10,1), 
    top_product_category VARCHAR NOT NULL, 
    top_craftsman_id BIGINT NOT NULL, 
    count_order_created BIGINT NOT NULL, 
    count_order_in_progress BIGINT NOT NULL, 
    count_order_delivery BIGINT NOT NULL,
    count_order_done BIGINT NOT NULL,
    count_order_not_done BIGINT NOT NULL, 
    report_period VARCHAR NOT NULL
);

-- Добавление комментариев для пользователей витрины
COMMENT ON COLUMN dwh.customer_report_datamart.id IS 'Уникальный идентификатор записи';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_id IS 'Идентификатор заказчика';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_name IS 'Ф.И.О. заказчика';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_address IS 'Адрес заказчика';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_birthday IS 'Дата рождения заказчика';
COMMENT ON COLUMN dwh.customer_report_datamart.customer_email IS 'Электронная почта заказчика';
COMMENT ON COLUMN dwh.customer_report_datamart.total_spent IS 'Сумма, которую потратил заказчик за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.platform_earnings IS 'Доход платформы от заказов клиента за месяц (10% от total_spent)';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order IS 'Количество заказов у заказчика за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.avg_order_price IS 'Средняя стоимость одного заказа у заказчика за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.median_time_order_completed IS 'Медианное время в днях от создания заказа до его завершения за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.top_product_category IS 'Самая популярная категория товаров у заказчика за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.top_craftsman_id IS 'Идентификатор самого популярного мастера у заказчика (при равенстве частот можно выбрать любого)';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_created IS 'Количество созданных заказов за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_in_progress IS 'Количество заказов в процессе за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_delivery IS 'Количество заказов в доставке за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_done IS 'Количество завершённых заказов за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.count_order_not_done IS 'Количество незавершённых заказов за месяц';
COMMENT ON COLUMN dwh.customer_report_datamart.report_period IS 'Отчётный период (год и месяц)';