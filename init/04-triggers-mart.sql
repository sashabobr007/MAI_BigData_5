-- Создаем таблицу для отслеживания выполнения скрипта
CREATE TABLE IF NOT EXISTS script_execution_log (
    id SERIAL PRIMARY KEY,
    script_name VARCHAR(100),
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_count INTEGER
);


-- Функция для проверки количества строк и выполнения скрипта
CREATE OR REPLACE FUNCTION check_and_execute_script()
RETURNS TRIGGER AS $$
DECLARE
    current_count INTEGER;
    already_executed BOOLEAN;
BEGIN
    -- Получаем текущее количество строк
    SELECT COUNT(*) INTO current_count FROM fact_sales;

    -- Проверяем, выполнялся ли уже скрипт для 10000 строк
    SELECT EXISTS (
        SELECT 1 FROM script_execution_log
        WHERE script_name = '10000_rows_script'
        AND rows_count = 10000
    ) INTO already_executed;

    -- Если достигли 10000 строк и скрипт еще не выполнялся
    IF current_count >= 10000 AND NOT already_executed THEN

					-- Топ-10 самых продаваемых продуктов
			CREATE VIEW product_top_10_sales AS
			SELECT
			    p.product_id,
			    p.name as product_name,
			    p.category,
			    SUM(fs.quantity) as total_quantity_sold,
			    SUM(fs.total_price) as total_revenue,
			    COUNT(fs.sale_id) as number_of_sales
			FROM fact_sales fs
			JOIN dim_products p ON fs.product_id = p.product_id
			GROUP BY p.product_id, p.name, p.category
			ORDER BY total_quantity_sold DESC
			LIMIT 10;

			-- Общая выручка по категориям продуктов
			CREATE VIEW product_category_revenue AS
			SELECT
			    p.category,
			    SUM(fs.total_price) as total_revenue,
			    SUM(fs.quantity) as total_quantity,
			    COUNT(fs.sale_id) as number_of_sales,
			    ROUND(SUM(fs.total_price) / SUM(fs.quantity), 2) as avg_price_per_item
			FROM fact_sales fs
			JOIN dim_products p ON fs.product_id = p.product_id
			GROUP BY p.category
			ORDER BY total_revenue DESC;

			-- Средний рейтинг и количество отзывов для каждого продукта
			CREATE VIEW product_ratings_reviews AS
			SELECT
			    p.product_id,
			    p.name as product_name,
			    p.category,
			    p.rating,
			    p.reviews,
			    SUM(fs.quantity) as total_quantity_sold,
			    SUM(fs.total_price) as total_revenue
			FROM dim_products p
			LEFT JOIN fact_sales fs ON p.product_id = fs.product_id
			GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
			ORDER BY p.rating DESC;



			-- Топ-10 клиентов с наибольшей общей суммой покупок
			CREATE VIEW customer_top_10_spenders AS
			SELECT
			    c.customer_id,
			    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
			    c.country,
			    SUM(fs.total_price) as total_spent,
			    COUNT(fs.sale_id) as total_orders,
			    ROUND(AVG(fs.total_price), 2) as avg_order_value
			FROM fact_sales fs
			JOIN dim_customers c ON fs.customer_id = c.customer_id
			GROUP BY c.customer_id, c.first_name, c.last_name, c.country
			ORDER BY total_spent DESC
			LIMIT 10;


			-- Распределение клиентов по странам
			CREATE VIEW customer_distribution_by_country AS
			SELECT
			    country,
			    COUNT(DISTINCT customer_id) as customer_count,
			    SUM(total_spent) as total_revenue,
			    ROUND(AVG(total_spent), 2) as avg_customer_value
			FROM (
			    SELECT
			        c.customer_id,
			        c.country,
			        SUM(fs.total_price) as total_spent
			    FROM dim_customers c
			    JOIN fact_sales fs ON c.customer_id = fs.customer_id
			    GROUP BY c.customer_id, c.country
			) customer_stats
			GROUP BY country
			ORDER BY customer_count DESC;



			-- Средний чек для каждого клиента
			CREATE VIEW customer_avg_order_value AS
			SELECT
			    c.customer_id,
			    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
			    c.country,
			    COUNT(fs.sale_id) as total_orders,
			    SUM(fs.total_price) as total_spent,
			    ROUND(AVG(fs.total_price), 2) as avg_order_value,
			    MAX(fs.sale_date) as last_purchase_date
			FROM fact_sales fs
			JOIN dim_customers c ON fs.customer_id = c.customer_id
			GROUP BY c.customer_id, c.first_name, c.last_name, c.country
			HAVING COUNT(fs.sale_id) > 0;


			-- Месячные и годовые тренды продаж
			CREATE VIEW sales_time_trends AS
			SELECT
			    DATE_TRUNC('year', fs.sale_date) as sale_year,
			    DATE_TRUNC('month', fs.sale_date) as sale_month,
			    TO_CHAR(fs.sale_date, 'YYYY-MM') as year_month,
			    SUM(fs.total_price) as monthly_revenue,
			    SUM(fs.quantity) as monthly_quantity,
			    COUNT(fs.sale_id) as monthly_orders,
			    LAG(SUM(fs.total_price)) OVER (ORDER BY DATE_TRUNC('month', fs.sale_date)) as prev_month_revenue,
			    ROUND(
			        (SUM(fs.total_price) - LAG(SUM(fs.total_price)) OVER (ORDER BY DATE_TRUNC('month', fs.sale_date))) /
			        LAG(SUM(fs.total_price)) OVER (ORDER BY DATE_TRUNC('month', fs.sale_date)) * 100, 2
			    ) as revenue_growth_percent
			FROM fact_sales fs
			GROUP BY DATE_TRUNC('year', fs.sale_date), DATE_TRUNC('month', fs.sale_date), TO_CHAR(fs.sale_date, 'YYYY-MM')
			ORDER BY sale_month;

			-- Сравнение выручки за разные периоды (помесячно)
			CREATE VIEW sales_monthly_comparison AS
			SELECT
			    EXTRACT(YEAR FROM fs.sale_date) as sale_year,
			    EXTRACT(MONTH FROM fs.sale_date) as sale_month,
			    TO_CHAR(fs.sale_date, 'Month') as month_name,
			    SUM(fs.total_price) as revenue,
			    SUM(fs.quantity) as quantity_sold,
			    COUNT(fs.sale_id) as order_count
			FROM fact_sales fs
			GROUP BY EXTRACT(YEAR FROM fs.sale_date), EXTRACT(MONTH FROM fs.sale_date), TO_CHAR(fs.sale_date, 'Month')
			ORDER BY sale_year, sale_month;

			-- Средний размер заказа по месяцам
			CREATE VIEW avg_order_size_monthly AS
			SELECT
			    DATE_TRUNC('month', fs.sale_date) as sale_month,
			    TO_CHAR(fs.sale_date, 'YYYY-MM') as year_month,
			    ROUND(AVG(fs.total_price), 2) as avg_order_value,
			    ROUND(AVG(fs.quantity), 2) as avg_items_per_order,
			    COUNT(fs.sale_id) as total_orders
			FROM fact_sales fs
			GROUP BY DATE_TRUNC('month', fs.sale_date), TO_CHAR(fs.sale_date, 'YYYY-MM')
			ORDER BY sale_month;


			-- Топ-5 магазинов с наибольшей выручкой
			CREATE VIEW store_top_5_revenue AS
			SELECT
			    s.store_id,
			    s.name as store_name,
			    s.city,
			    s.country,
			    SUM(fs.total_price) as total_revenue,
			    COUNT(fs.sale_id) as total_sales,
			    SUM(fs.quantity) as total_quantity_sold,
			    ROUND(SUM(fs.total_price) / COUNT(fs.sale_id), 2) as avg_sale_value
			FROM fact_sales fs
			JOIN dim_stores s ON fs.store_id = s.store_id
			GROUP BY s.store_id, s.name, s.city, s.country
			ORDER BY total_revenue DESC
			LIMIT 5;



			-- Распределение продаж по городам и странам
			CREATE VIEW store_sales_by_location AS
			SELECT
			    s.country,
			    s.city,
			    COUNT(DISTINCT s.store_id) as store_count,
			    SUM(fs.total_price) as total_revenue,
			    COUNT(fs.sale_id) as total_sales,
			    ROUND(SUM(fs.total_price) / COUNT(fs.sale_id), 2) as avg_sale_value
			FROM fact_sales fs
			JOIN dim_stores s ON fs.store_id = s.store_id
			GROUP BY s.country, s.city
			ORDER BY total_revenue DESC;



			-- Средний чек для каждого магазина
			CREATE VIEW store_avg_order_value AS
			SELECT
			    s.store_id,
			    s.name as store_name,
			    s.city,
			    s.country,
			    COUNT(fs.sale_id) as total_orders,
			    SUM(fs.total_price) as total_revenue,
			    ROUND(AVG(fs.total_price), 2) as avg_order_value,
			    ROUND(MIN(fs.total_price), 2) as min_order_value,
			    ROUND(MAX(fs.total_price), 2) as max_order_value
			FROM fact_sales fs
			JOIN dim_stores s ON fs.store_id = s.store_id
			GROUP BY s.store_id, s.name, s.city, s.country;


			-- Топ-5 поставщиков с наибольшей выручкой
			CREATE VIEW supplier_top_5_revenue AS
			SELECT
			    sup.supplier_id,
			    sup.name as supplier_name,
			    sup.country,
			    SUM(fs.total_price) as total_revenue,
			    SUM(fs.quantity) as total_quantity_sold,
			    COUNT(fs.sale_id) as total_sales,
			    ROUND(AVG(p.price), 2) as avg_product_price
			FROM fact_sales fs
			JOIN dim_suppliers sup ON fs.supplier_id = sup.supplier_id
			JOIN dim_products p ON fs.product_id = p.product_id
			GROUP BY sup.supplier_id, sup.name, sup.country
			ORDER BY total_revenue DESC
			LIMIT 5;

			-- Средняя цена товаров от каждого поставщика
			CREATE VIEW supplier_avg_prices AS
			SELECT
			    sup.supplier_id,
			    sup.name as supplier_name,
			    sup.country,
			    COUNT(DISTINCT p.product_id) as product_count,
			    ROUND(AVG(p.price), 2) as avg_product_price,
			    MIN(p.price) as min_product_price,
			    MAX(p.price) as max_product_price,
			    SUM(fs.total_price) as total_revenue
			FROM dim_suppliers sup
			JOIN fact_sales fs ON sup.supplier_id = fs.supplier_id
			JOIN dim_products p ON fs.product_id = p.product_id
			GROUP BY sup.supplier_id, sup.name, sup.country
			ORDER BY avg_product_price DESC;

			-- Распределение продаж по странам поставщиков
			CREATE VIEW supplier_sales_by_country AS
			SELECT
			    sup.country,
			    COUNT(DISTINCT sup.supplier_id) as supplier_count,
			    SUM(fs.total_price) as total_revenue,
			    SUM(fs.quantity) as total_quantity_sold,
			    ROUND(SUM(fs.total_price) / SUM(fs.quantity), 2) as avg_price_per_item
			FROM fact_sales fs
			JOIN dim_suppliers sup ON fs.supplier_id = sup.supplier_id
			GROUP BY sup.country
			ORDER BY total_revenue DESC;



			-- Продукты с наивысшим и наименьшим рейтингом
			CREATE VIEW product_rating_high AS
			SELECT
			    product_id,
			    name as product_name,
			    category,
			    rating,
			    reviews,
			    total_quantity_sold,
			    total_revenue
			FROM (
			    -- Топ-10 продуктов с наивысшим рейтингом
			    SELECT
			        p.product_id,
			        p.name,
			        p.category,
			        p.rating,
			        p.reviews,
			        SUM(fs.quantity) as total_quantity_sold,
			        SUM(fs.total_price) as total_revenue,
			        'Highest Rated' as rating_category
			    FROM dim_products p
			    LEFT JOIN fact_sales fs ON p.product_id = fs.product_id
			    WHERE p.rating IS NOT NULL
			    GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
			    ORDER BY p.rating DESC, p.reviews DESC
			    LIMIT 10) combined_ratings
			ORDER BY rating_category, rating DESC;



			CREATE VIEW product_rating_low AS
			SELECT
			    product_id,
			    name as product_name,
			    category,
			    rating,
			    reviews,
			    total_quantity_sold,
			    total_revenue
			FROM (
			    SELECT
			        p.product_id,
			        p.name,
			        p.category,
			        p.rating,
			        p.reviews,
			        SUM(fs.quantity) as total_quantity_sold,
			        SUM(fs.total_price) as total_revenue,
			        'Lowest Rated' as rating_category
			    FROM dim_products p
			    LEFT JOIN fact_sales fs ON p.product_id = fs.product_id
			    WHERE p.rating IS NOT NULL AND p.rating > 0
			    GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
			    ORDER BY p.rating ASC, p.reviews DESC
			    LIMIT 10
			) combined_ratings
			ORDER BY rating_category, rating DESC;




			-- Корреляция между рейтингом и объемом продаж
			CREATE VIEW product_rating_sales_correlation AS
			SELECT
			    p.product_id,
			    p.name as product_name,
			    p.category,
			    p.rating,
			    p.reviews,
			    SUM(fs.quantity) as total_quantity_sold,
			    SUM(fs.total_price) as total_revenue,
			    CASE
			        WHEN p.rating >= 4.5 THEN 'Excellent'
			        WHEN p.rating >= 4.0 THEN 'Very Good'
			        WHEN p.rating >= 3.5 THEN 'Good'
			        WHEN p.rating >= 3.0 THEN 'Average'
			        ELSE 'Poor'
			    END as rating_category
			FROM dim_products p
			LEFT JOIN fact_sales fs ON p.product_id = fs.product_id
			WHERE p.rating IS NOT NULL
			GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
			ORDER BY total_quantity_sold DESC;



			-- Продукты с наибольшим количеством отзывов
			CREATE VIEW product_most_reviews AS
			SELECT
			    p.product_id,
			    p.name as product_name,
			    p.category,
			    p.rating,
			    p.reviews,
			    SUM(fs.quantity) as total_quantity_sold,
			    SUM(fs.total_price) as total_revenue,
			    ROUND(SUM(fs.quantity) / NULLIF(p.reviews, 0), 2) as sales_per_review
			FROM dim_products p
			LEFT JOIN fact_sales fs ON p.product_id = fs.product_id
			WHERE p.reviews > 0
			GROUP BY p.product_id, p.name, p.category, p.rating, p.reviews
			ORDER BY p.reviews DESC
			LIMIT 15;

        -- Логируем выполнение
        INSERT INTO script_execution_log (script_name, rows_count)
        VALUES ('10000_rows_script', 10000);

        RAISE NOTICE 'Скрипт выполнен для 10000 строк в fact_sales';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;




-- Создаем триггер
CREATE OR REPLACE TRIGGER check_rows_trigger
    AFTER INSERT ON fact_sales
    FOR EACH ROW
    EXECUTE FUNCTION check_and_execute_script();