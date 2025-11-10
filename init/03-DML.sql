-- Сначала создаем триггерную функцию для обработки одной строки
CREATE OR REPLACE FUNCTION process_mock_data_insert_single()
RETURNS TRIGGER AS $$
DECLARE
    v_customer_id INTEGER;
    v_seller_id INTEGER;
    v_product_id INTEGER;
    v_store_id INTEGER;
    v_supplier_id INTEGER;
BEGIN
    -- Заполнение измерения клиентов (только для новой строки)
    INSERT INTO dim_customers (first_name, last_name, age, email, country, postal_code)
    VALUES (
        NEW.customer_first_name,
        NEW.customer_last_name,
        NEW.customer_age,
        NEW.customer_email,
        NEW.customer_country,
        NEW.customer_postal_code
    )
    ON CONFLICT (first_name, last_name, email, age, country) DO NOTHING
    RETURNING customer_id INTO v_customer_id;

    -- Если конфликт, получаем существующий customer_id
    IF v_customer_id IS NULL THEN
        SELECT customer_id INTO v_customer_id
        FROM dim_customers
        WHERE first_name = NEW.customer_first_name
          AND last_name = NEW.customer_last_name
          AND email = NEW.customer_email
          AND age = NEW.customer_age
          AND country = NEW.customer_country;
    END IF;

    -- Заполнение измерения продавцов (только для новой строки)
    INSERT INTO dim_sellers (first_name, last_name, email, country, postal_code)
    VALUES (
        NEW.seller_first_name,
        NEW.seller_last_name,
        NEW.seller_email,
        NEW.seller_country,
        NEW.seller_postal_code
    )
    ON CONFLICT (first_name, last_name, email, country) DO NOTHING
    RETURNING seller_id INTO v_seller_id;

    -- Если конфликт, получаем существующий seller_id
    IF v_seller_id IS NULL THEN
        SELECT seller_id INTO v_seller_id
        FROM dim_sellers
        WHERE first_name = NEW.seller_first_name
          AND last_name = NEW.seller_last_name
          AND email = NEW.seller_email
          AND country = NEW.seller_country;
    END IF;

    -- Заполнение измерения продуктов с преобразованием дат
    INSERT INTO dim_products (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date, pet_category)
    VALUES (
        NEW.product_name,
        NEW.product_category,
        NEW.product_price,
        NEW.product_weight,
        NEW.product_color,
        NEW.product_size,
        NEW.product_brand,
        NEW.product_material,
        NEW.product_description,
        NEW.product_rating,
        NEW.product_reviews,
        TO_DATE(NEW.product_release_date, 'MM/DD/YYYY'),
        TO_DATE(NEW.product_expiry_date, 'MM/DD/YYYY'),
        NEW.pet_category
    )
    ON CONFLICT (name, category, price, weight, color, size, brand, material, description, rating, reviews, release_date, expiry_date) DO NOTHING
    RETURNING product_id INTO v_product_id;

    -- Если конфликт, получаем существующий product_id
    IF v_product_id IS NULL THEN
        SELECT product_id INTO v_product_id
        FROM dim_products
        WHERE name = NEW.product_name
          AND category = NEW.product_category
          AND price = NEW.product_price
          AND weight = NEW.product_weight
          AND color = NEW.product_color
          AND size = NEW.product_size
          AND brand = NEW.product_brand
          AND material = NEW.product_material
          AND description = NEW.product_description
          AND rating = NEW.product_rating
          AND reviews = NEW.product_reviews
          AND release_date = TO_DATE(NEW.product_release_date, 'MM/DD/YYYY')
          AND expiry_date = TO_DATE(NEW.product_expiry_date, 'MM/DD/YYYY');
    END IF;

    -- Заполнение измерения магазинов
    INSERT INTO dim_stores (name, location, city, state, country, phone, email)
    VALUES (
        NEW.store_name,
        NEW.store_location,
        NEW.store_city,
        NEW.store_state,
        NEW.store_country,
        NEW.store_phone,
        NEW.store_email
    )
    ON CONFLICT (name, location, city, country) DO NOTHING
    RETURNING store_id INTO v_store_id;

    -- Если конфликт, получаем существующий store_id
    IF v_store_id IS NULL THEN
        SELECT store_id INTO v_store_id
        FROM dim_stores
        WHERE name = NEW.store_name
          AND location = NEW.store_location
          AND city = NEW.store_city
          AND country = NEW.store_country;
    END IF;

    -- Заполнение измерения поставщиков
    INSERT INTO dim_suppliers (name, contact, email, phone, address, city, country)
    VALUES (
        NEW.supplier_name,
        NEW.supplier_contact,
        NEW.supplier_email,
        NEW.supplier_phone,
        NEW.supplier_address,
        NEW.supplier_city,
        NEW.supplier_country
    )
    ON CONFLICT (name, contact, email) DO NOTHING
    RETURNING supplier_id INTO v_supplier_id;

    -- Если конфликт, получаем существующий supplier_id
    IF v_supplier_id IS NULL THEN
        SELECT supplier_id INTO v_supplier_id
        FROM dim_suppliers
        WHERE name = NEW.supplier_name
          AND contact = NEW.supplier_contact
          AND email = NEW.supplier_email;
    END IF;

    -- Заполнение измерения питомцев (используем полученный customer_id)
    INSERT INTO dim_pets (customer_id, pet_type, pet_name, pet_breed)
    VALUES (
        v_customer_id,
        NEW.customer_pet_type,
        NEW.customer_pet_name,
        NEW.customer_pet_breed
    )
    ON CONFLICT (customer_id, pet_name, pet_breed) DO NOTHING;

    -- Заполнение таблицы фактов с преобразованием даты продажи
    INSERT INTO fact_sales (customer_id, seller_id, product_id, store_id, supplier_id, sale_date, quantity, total_price, unit_price)
    VALUES (
        v_customer_id,
        v_seller_id,
        v_product_id,
        v_store_id,
        v_supplier_id,
        TO_DATE(NEW.sale_date, 'MM/DD/YYYY'),
        NEW.sale_quantity,
        NEW.sale_total_price,
        NEW.sale_total_price / NULLIF(NEW.sale_quantity, 0)
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;



-- Создаем триггер для обработки каждой строки
CREATE OR REPLACE TRIGGER mock_data_insert_single_trigger
AFTER INSERT ON mock_data
FOR EACH ROW
EXECUTE FUNCTION process_mock_data_insert_single();