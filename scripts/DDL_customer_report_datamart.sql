WITH
dwh_delta AS (
    SELECT
        dcs.customer_id,
        dcs.customer_name,
        dcs.customer_address,
        dcs.customer_birthday,
        dcs.customer_email,
        fo.order_id,
        dp.product_id,
        dp.product_price,
        dp.product_type,
        fo.order_completion_date - fo.order_created_date AS diff_order_date, 
        fo.order_status,
        fo.craftsman_id,
        TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
        crd.customer_id AS exist_customer_id,
        fo.load_dttm AS order_load_dttm,
        dcs.load_dttm AS customer_load_dttm,
        dp.load_dttm AS product_load_dttm
    FROM dwh.f_order fo
    INNER JOIN dwh.d_customer dcs USING (customer_id)
    INNER JOIN dwh.d_product dp USING (product_id) 
    LEFT JOIN dwh.customer_report_datamart crd USING (customer_id) 
    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
          (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),

--- вычисляю топов
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


dwh_delta_insert_result AS ( 
   SELECT  
        t1.customer_id,
        t1.customer_name,
        t1.customer_address,
        t1.customer_birthday,
        t1.customer_email,
        SUM(t1.product_price) AS total_spent,
        SUM(t1.product_price) * 0.1 AS platform_earnings,
        COUNT(t1.order_id) AS count_order,
        AVG(t1.product_price) AS avg_order_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t1.diff_order_date) AS median_time_order_completed,
        tp.product_type AS top_product_category,
        tc.craftsman_id AS top_craftsman_id,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'created') AS count_order_created,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'in_progress') AS count_order_in_progress,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'delivery') AS count_order_delivery,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'done') AS count_order_done,
        COUNT(order_id) FILTER (WHERE t1.order_status <> 'done') AS count_order_not_done,
        t1.report_period
    FROM dwh_delta AS t1
    
    INNER JOIN top_product tp ON tp.customer_id = t1.customer_id 
    	AND tp.report_period = t1.report_period
    	AND ranked_product = 1
    
    INNER JOIN top_craftsmen tc ON tc.customer_id = t1.customer_id
    	AND ranked_craftsman = 1
    WHERE exist_customer_id IS NULL
    GROUP BY t1.customer_id, t1.customer_name, t1.customer_address, t1.customer_birthday, 
             t1.customer_email, tp.product_type, tc.craftsman_id, t1.report_period),
             
          --обновление существующих  
dwh_delta_update_result AS ( 
     SELECT  
        T1.customer_id,
        T1.customer_name,
        T1.customer_address,
        T1.customer_birthday,
        T1.customer_email,
        SUM(T1.product_price) AS total_spent,
        SUM(T1.product_price) * 0.1 AS platform_earnings,
        COUNT(T1.order_id) AS count_order,
        AVG(T1.product_price) AS avg_order_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY T1.diff_order_date) AS median_time_order_completed,
        tp.product_type as top_product_category,
        tc.craftsman_id as top_craftsman_id,      
        COUNT(order_id) FILTER (WHERE t1.order_status = 'created') AS count_order_created,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'in_progress') AS count_order_in_progress,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'delivery') AS count_order_delivery,
        COUNT(order_id) FILTER (WHERE t1.order_status = 'done') AS count_order_done,
        COUNT(order_id) FILTER (WHERE t1.order_status <> 'done') AS count_order_not_done,
        T1.report_period
    FROM dwh_delta AS T1
    
    INNER JOIN top_product AS tp ON tp.customer_id = t1.customer_id
    		AND tp.report_period = t1.report_period
    		AND ranked_product = 1
    INNER JOIN top_craftsmen as tc ON tc.customer_id = t1.customer_id
    	AND ranked_craftsman = 1
    WHERE exist_customer_id NOTNULL
    GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, 
             T1.customer_email, tp.product_type, tc.craftsman_id, T1.report_period
),


insert_delta AS ( 
    INSERT INTO dwh.customer_report_datamart (
        customer_id,
        customer_name,
        customer_address,
        customer_birthday, 
        customer_email, 
        total_spent, 
        platform_earnings, 
        count_order, 
        avg_order_price, 
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created, 
        count_order_in_progress, 
        count_order_delivery, 
        count_order_done, 
        count_order_not_done, 
        report_period
    )
    SELECT 
        customer_id,
        customer_name,
        customer_address,
        customer_birthday,
        customer_email,
        total_spent,
        platform_earnings,
        count_order,
        avg_order_price,
        median_time_order_completed,
        top_product_category,
        top_craftsman_id,
        count_order_created, 
        count_order_in_progress,
        count_order_delivery, 
        count_order_done, 
        count_order_not_done,
        report_period 
    FROM dwh_delta_insert_result
),

update_delta AS ( 
    UPDATE dwh.customer_report_datamart
    SET
        customer_name = updates.customer_name, 
        customer_address = updates.customer_address, 
        customer_birthday = updates.customer_birthday, 
        customer_email = updates.customer_email, 
        total_spent = updates.total_spent, 
        platform_earnings = updates.platform_earnings, 
        count_order = updates.count_order, 
        avg_order_price = updates.avg_order_price, 
        median_time_order_completed = updates.median_time_order_completed, 
        top_product_category = updates.top_product_category, 
        top_craftsman_id = updates.top_craftsman_id, 
        count_order_created = updates.count_order_created, 
        count_order_in_progress = updates.count_order_in_progress, 
        count_order_delivery = updates.count_order_delivery, 
        count_order_done = updates.count_order_done,
        count_order_not_done = updates.count_order_not_done, 
        report_period = updates.report_period
    FROM dwh_delta_update_result AS updates
    WHERE dwh.customer_report_datamart using (customer_id)
),

insert_load_date AS ( 
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT GREATEST(COALESCE(MAX(order_load_dttm), NOW()), 
                    COALESCE(MAX(customer_load_dttm), NOW()), 
                    COALESCE(MAX(product_load_dttm), NOW())) 
    FROM dwh_delta
)

SELECT 'increment datamart';