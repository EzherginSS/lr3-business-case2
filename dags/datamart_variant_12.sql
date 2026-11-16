-- Витрина данных варианта 12 "Анализ онлайн-продаж".
-- Обязательные поля варианта: invoice_date, country, stock_code, description, revenue.
-- Дополнительные поля нужны для подсчета заказов (комбинированный чарт) и фильтров.
-- revenue = quantity * unit_price (чистая выручка: отмены входят с минусом).

DROP VIEW IF EXISTS online_retail_datamart;

CREATE VIEW online_retail_datamart AS
SELECT
    s.invoice_date,                                   -- дата счета (DATE)
    s.country,                                        -- страна покупателя
    s.stock_code,                                     -- код товара
    s.description,                                    -- наименование товара
    s.revenue,                                        -- выручка строки, GBP
    s.invoice_no,                                     -- номер счета
    CASE WHEN s.is_cancellation THEN NULL
         ELSE s.invoice_no END        AS order_no,    -- номер заказа (без отмен)
    s.is_cancellation,                                -- признак отмены/возврата
    s.quantity,                                       -- количество, шт.
    s.unit_price                                      -- цена за единицу, GBP
FROM stg_online_retail s;

COMMENT ON VIEW online_retail_datamart IS
    'Вариант 12. Витрина онлайн-продаж для Superset: revenue = quantity * unit_price, отмены с минусом';
