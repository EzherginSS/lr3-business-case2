-- STG-слой варианта 12: приведение типов, нормализация, отбраковка с журналом.
-- Скрипт идемпотентен: при повторном запуске DAG слой пересчитывается из RAW.

DROP VIEW  IF EXISTS online_retail_datamart;
DROP TABLE IF EXISTS stg_online_retail;
DROP TABLE IF EXISTS stg_online_retail_rejects;

-- 1. Типизация и нормализация всех строк RAW (временная таблица сеанса)
CREATE TEMP TABLE tmp_typed AS
SELECT
    r.source_row_num,
    upper(btrim(r.invoice_no))                                         AS invoice_no,
    upper(btrim(r.stock_code))                                         AS stock_code,
    NULLIF(upper(regexp_replace(btrim(r.description), '\s+', ' ', 'g')), '') AS description,
    r.quantity::numeric::integer                                       AS quantity,
    r.invoice_date::timestamp                                          AS invoice_ts,
    r.unit_price::numeric                                              AS unit_price,
    NULLIF(btrim(r.customer_id), '')::numeric::integer                 AS customer_id,
    CASE btrim(r.country)
        WHEN 'EIRE' THEN 'Ireland'
        WHEN 'RSA'  THEN 'South Africa'
        WHEN 'USA'  THEN 'United States'
        ELSE regexp_replace(btrim(r.country), '\s+', ' ', 'g')
    END                                                                AS country,
    -- ключ для поиска полных дублей строк источника
    md5(ROW(r.invoice_no, r.stock_code, r.description, r.quantity,
            r.invoice_date, r.unit_price, r.customer_id, r.country)::text) AS row_hash
FROM raw_online_retail r;

-- 2. Журнал отбраковки: каждая удаленная строка сохраняется с причиной
CREATE TABLE stg_online_retail_rejects (
    source_row_num INTEGER PRIMARY KEY,
    reject_reason  TEXT    NOT NULL,
    invoice_no     TEXT,
    stock_code     TEXT,
    description    TEXT,
    quantity       INTEGER,
    invoice_ts     TIMESTAMP,
    unit_price     NUMERIC,
    customer_id    INTEGER,
    country        TEXT,
    rejected_at    TIMESTAMP NOT NULL DEFAULT now()
);

-- Правило 1. Цена <= 0: корректировки остатков, списания, "Adjust bad debt".
--            Это не продажа, выручки по таким строкам нет.
INSERT INTO stg_online_retail_rejects
    (source_row_num, reject_reason, invoice_no, stock_code, description,
     quantity, invoice_ts, unit_price, customer_id, country)
SELECT source_row_num, 'R1_non_positive_unit_price', invoice_no, stock_code, description,
       quantity, invoice_ts, unit_price, customer_id, country
FROM tmp_typed
WHERE unit_price <= 0;

-- Правило 2. Служебные коды, не являющиеся товаром: комиссии и сборы, ручные
--            корректировки, скидки, образцы, доставка.
INSERT INTO stg_online_retail_rejects
    (source_row_num, reject_reason, invoice_no, stock_code, description,
     quantity, invoice_ts, unit_price, customer_id, country)
SELECT t.source_row_num, 'R2_service_stock_code', t.invoice_no, t.stock_code, t.description,
       t.quantity, t.invoice_ts, t.unit_price, t.customer_id, t.country
FROM tmp_typed t
WHERE t.unit_price > 0
  AND t.stock_code IN ('AMAZONFEE', 'BANK CHARGES', 'CRUK', 'B', 'M', 'D', 'S',
                       'PADS', 'POST', 'DOT', 'C2');

-- Правило 3. Полные дубли строки источника: остается первое вхождение.
INSERT INTO stg_online_retail_rejects
    (source_row_num, reject_reason, invoice_no, stock_code, description,
     quantity, invoice_ts, unit_price, customer_id, country)
SELECT d.source_row_num, 'R3_exact_duplicate', d.invoice_no, d.stock_code, d.description,
       d.quantity, d.invoice_ts, d.unit_price, d.customer_id, d.country
FROM (
    SELECT t.*,
           row_number() OVER (PARTITION BY t.row_hash ORDER BY t.source_row_num) AS rn
    FROM tmp_typed t
    WHERE NOT EXISTS (SELECT 1 FROM stg_online_retail_rejects x
                      WHERE x.source_row_num = t.source_row_num)
) d
WHERE d.rn > 1;

-- 3. Единое описание товара: у одного stock_code в источнике бывает несколько
--    вариантов текста; берется самый частый (при равенстве - первый по алфавиту).
CREATE TEMP TABLE tmp_desc AS
SELECT stock_code, description
FROM (
    SELECT stock_code, description,
           row_number() OVER (PARTITION BY stock_code
                              ORDER BY count(*) DESC, description) AS rn
    FROM tmp_typed
    WHERE description IS NOT NULL
      AND unit_price > 0
    GROUP BY stock_code, description
) s
WHERE rn = 1;

-- 4. STG: очищенные и обогащенные строки
CREATE TABLE stg_online_retail AS
SELECT
    t.source_row_num,
    t.invoice_no,
    (t.invoice_no LIKE 'C%')                    AS is_cancellation,
    t.invoice_ts,
    t.invoice_ts::date                          AS invoice_date,
    t.stock_code,
    COALESCE(d.description, t.description)      AS description,
    t.quantity,
    t.unit_price,
    (t.quantity * t.unit_price)                 AS revenue,
    t.customer_id,
    t.country
FROM tmp_typed t
LEFT JOIN tmp_desc d ON d.stock_code = t.stock_code
WHERE NOT EXISTS (SELECT 1 FROM stg_online_retail_rejects x
                  WHERE x.source_row_num = t.source_row_num);

ALTER TABLE stg_online_retail ADD PRIMARY KEY (source_row_num);
CREATE INDEX ix_stg_online_retail_date    ON stg_online_retail (invoice_date);
CREATE INDEX ix_stg_online_retail_country ON stg_online_retail (country);

-- 5. Итоги шага в журнал качества
INSERT INTO etl_quality_log (step, rule, rows_affected, revenue_affected, comment)
SELECT 'transform_stg', reject_reason, count(*), sum(quantity * unit_price), 'rows rejected'
FROM stg_online_retail_rejects
GROUP BY reject_reason;

INSERT INTO etl_quality_log (step, rule, rows_affected, revenue_affected, comment)
SELECT 'transform_stg', 'stg_rows', count(*), sum(revenue), 'rows kept'
FROM stg_online_retail;

INSERT INTO etl_quality_log (step, rule, rows_affected, revenue_affected, comment)
SELECT 'transform_stg', 'cancellation_rows_kept', count(*), sum(revenue),
       'cancellations kept with negative revenue (net revenue rule)'
FROM stg_online_retail
WHERE is_cancellation;

DROP TABLE tmp_desc;
DROP TABLE tmp_typed;
