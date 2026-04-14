-- ============================================================
-- 05_synthetic_data.sql
-- Run as: SYSADMIN
-- Generates 4.5M rows of synthetic data across 7 domains
-- ============================================================

USE ROLE SYSADMIN;
USE DATABASE CUSTOMER_PLATFORM;
USE SCHEMA RAW;
USE WAREHOUSE WH_INGEST;

-- CUSTOMERS (50K)
INSERT INTO RAW.CUSTOMERS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 50000))
)
SELECT
    'CUST_' || LPAD(rn, 6, '0'),
    CASE MOD(rn,20) WHEN 0 THEN 'James' WHEN 1 THEN 'Mary' WHEN 2 THEN 'Robert'
        WHEN 3 THEN 'Patricia' WHEN 4 THEN 'John' WHEN 5 THEN 'Jennifer'
        WHEN 6 THEN 'Michael' WHEN 7 THEN 'Linda' WHEN 8 THEN 'William'
        WHEN 9 THEN 'Barbara' WHEN 10 THEN 'David' WHEN 11 THEN 'Susan'
        WHEN 12 THEN 'Richard' WHEN 13 THEN 'Jessica' WHEN 14 THEN 'Joseph'
        WHEN 15 THEN 'Sarah' WHEN 16 THEN 'Thomas' WHEN 17 THEN 'Karen'
        WHEN 18 THEN 'Charles' ELSE 'Lisa' END,
    CASE MOD(rn,10) WHEN 0 THEN 'Smith' WHEN 1 THEN 'Johnson'
        WHEN 2 THEN 'Williams' WHEN 3 THEN 'Brown' WHEN 4 THEN 'Jones'
        WHEN 5 THEN 'Garcia' WHEN 6 THEN 'Miller' WHEN 7 THEN 'Davis'
        WHEN 8 THEN 'Martinez' ELSE 'Wilson' END,
    'customer_' || rn || '@email.com',
    '+1555' || LPAD(MOD(rn*7,9999999),7,'0'),
    DATEADD('day', -UNIFORM(6570,22000,RANDOM()), CURRENT_DATE()),
    CASE MOD(rn,3) WHEN 0 THEN 'male' WHEN 1 THEN 'female' ELSE 'other' END,
    DATEADD('day', -UNIFORM(1,1825,RANDOM()), CURRENT_DATE()),
    CASE MOD(rn,6) WHEN 0 THEN 'organic' WHEN 1 THEN 'paid_search'
        WHEN 2 THEN 'referral' WHEN 3 THEN 'social'
        WHEN 4 THEN 'email' ELSE 'direct' END,
    CASE MOD(rn,4) WHEN 0 THEN 'NORTHEAST' WHEN 1 THEN 'SOUTHEAST'
        WHEN 2 THEN 'WEST' ELSE 'MIDWEST' END,
    CASE MOD(rn,10) WHEN 0 THEN 'New York' WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago' WHEN 3 THEN 'Houston' WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Philadelphia' WHEN 6 THEN 'San Antonio'
        WHEN 7 THEN 'San Diego' WHEN 8 THEN 'Dallas' ELSE 'Miami' END,
    CASE MOD(rn,10) WHEN 0 THEN 'NY' WHEN 1 THEN 'CA' WHEN 2 THEN 'IL'
        WHEN 3 THEN 'TX' WHEN 4 THEN 'AZ' WHEN 5 THEN 'PA'
        WHEN 6 THEN 'TX' WHEN 7 THEN 'CA' WHEN 8 THEN 'TX' ELSE 'FL' END,
    LPAD(MOD(rn*13,99999),5,'0'),
    'US',
    CASE WHEN MOD(rn,10)=0 THEN 'platinum' WHEN MOD(rn,5)=0 THEN 'gold'
         WHEN MOD(rn,3)=0 THEN 'silver' ELSE 'bronze' END,
    CASE WHEN MOD(rn,20)=0 THEN FALSE ELSE TRUE END,
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- PRODUCTS (500)
INSERT INTO RAW.PRODUCTS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 500))
)
SELECT
    'PROD_' || LPAD(rn,4,'0'),
    CASE MOD(rn,8) WHEN 0 THEN 'Running Shoes' WHEN 1 THEN 'Wireless Headphones'
        WHEN 2 THEN 'Coffee Maker' WHEN 3 THEN 'Yoga Mat'
        WHEN 4 THEN 'Laptop Stand' WHEN 5 THEN 'Water Bottle'
        WHEN 6 THEN 'Backpack' ELSE 'Sunglasses' END || ' Model ' || rn,
    CASE MOD(rn,6) WHEN 0 THEN 'electronics' WHEN 1 THEN 'apparel'
        WHEN 2 THEN 'home' WHEN 3 THEN 'sports'
        WHEN 4 THEN 'beauty' ELSE 'accessories' END,
    CASE MOD(rn,6) WHEN 0 THEN 'phones' WHEN 1 THEN 'shirts'
        WHEN 2 THEN 'kitchen' WHEN 3 THEN 'fitness'
        WHEN 4 THEN 'skincare' ELSE 'bags' END,
    CASE MOD(rn,5) WHEN 0 THEN 'Nike' WHEN 1 THEN 'Apple'
        WHEN 2 THEN 'Samsung' WHEN 3 THEN 'Adidas' ELSE 'Generic' END,
    ROUND(UNIFORM(9,499,RANDOM())::FLOAT + UNIFORM(0,99,RANDOM())::FLOAT/100, 2),
    ROUND(UNIFORM(5,250,RANDOM())::FLOAT, 2),
    CASE WHEN UNIFORM(9,499,RANDOM()) < 30 THEN 'budget'
         WHEN UNIFORM(9,499,RANDOM()) < 100 THEN 'mid'
         WHEN UNIFORM(9,499,RANDOM()) < 250 THEN 'premium'
         ELSE 'luxury' END,
    TRUE,
    DATEADD('day', -UNIFORM(1,730,RANDOM()), CURRENT_DATE()),
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- ORDERS (500K)
INSERT INTO RAW.ORDERS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 500000))
)
SELECT
    'ORD_' || LPAD(rn,8,'0'),
    'CUST_' || LPAD(MOD(rn,50000)+1,6,'0'),
    DATEADD('second', -UNIFORM(1,31536000,RANDOM()), CURRENT_TIMESTAMP()),
    CASE WHEN MOD(rn,20)=0 THEN 'returned' WHEN MOD(rn,15)=0 THEN 'cancelled'
         WHEN MOD(rn,10)=0 THEN 'shipped' WHEN MOD(rn,5)=0 THEN 'delivered'
         ELSE 'confirmed' END,
    ROUND(UNIFORM(10,500,RANDOM())::FLOAT + UNIFORM(0,99,RANDOM())::FLOAT/100, 2),
    CASE WHEN MOD(rn,4)=0 THEN ROUND(UNIFORM(5,50,RANDOM())::FLOAT,2) ELSE 0 END,
    ROUND(UNIFORM(1,40,RANDOM())::FLOAT,2),
    CASE WHEN MOD(rn,3)=0 THEN 0 ELSE ROUND(UNIFORM(5,15,RANDOM())::FLOAT,2) END,
    CASE MOD(rn,5) WHEN 0 THEN 'credit_card' WHEN 1 THEN 'debit_card'
        WHEN 2 THEN 'paypal' WHEN 3 THEN 'wallet' ELSE 'bnpl' END,
    CASE MOD(rn,3) WHEN 0 THEN 'web' WHEN 1 THEN 'mobile_app' ELSE 'in_store' END,
    CASE WHEN MOD(rn,5)=0 THEN 'PROMO' || MOD(rn,20) ELSE NULL END,
    CASE MOD(rn,5) WHEN 0 THEN 'New York' WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago' WHEN 3 THEN 'Houston' ELSE 'Miami' END,
    CASE MOD(rn,5) WHEN 0 THEN 'NY' WHEN 1 THEN 'CA'
        WHEN 2 THEN 'IL' WHEN 3 THEN 'TX' ELSE 'FL' END,
    'US',
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- ORDER ITEMS (1M)
INSERT INTO RAW.ORDER_ITEMS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 1000000))
)
SELECT
    'ITEM_' || LPAD(rn,8,'0'),
    'ORD_' || LPAD(MOD(rn,500000)+1,8,'0'),
    'PROD_' || LPAD(MOD(rn,500)+1,4,'0'),
    UNIFORM(1,5,RANDOM()),
    ROUND(UNIFORM(9,299,RANDOM())::FLOAT + UNIFORM(0,99,RANDOM())::FLOAT/100, 2),
    CASE WHEN MOD(rn,5)=0 THEN ROUND(UNIFORM(1,30,RANDOM())::FLOAT,2) ELSE 0 END,
    ROUND(UNIFORM(9,299,RANDOM())::FLOAT * UNIFORM(1,5,RANDOM())::FLOAT, 2),
    CASE MOD(rn,6) WHEN 0 THEN 'electronics' WHEN 1 THEN 'apparel'
        WHEN 2 THEN 'home' WHEN 3 THEN 'sports'
        WHEN 4 THEN 'beauty' ELSE 'accessories' END,
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- TRANSACTIONS (750K)
INSERT INTO RAW.TRANSACTIONS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 750000))
)
SELECT
    'TXN_' || LPAD(rn,8,'0'),
    'ORD_' || LPAD(MOD(rn,500000)+1,8,'0'),
    'CUST_' || LPAD(MOD(rn,50000)+1,6,'0'),
    DATEADD('second', -UNIFORM(1,31536000,RANDOM()), CURRENT_TIMESTAMP()),
    ROUND(UNIFORM(10,500,RANDOM())::FLOAT, 2),
    'USD',
    CASE MOD(rn,5) WHEN 0 THEN 'credit_card' WHEN 1 THEN 'debit_card'
        WHEN 2 THEN 'paypal' WHEN 3 THEN 'wallet' ELSE 'bnpl' END,
    LPAD(MOD(rn*17,999999),6,'0'),
    CASE WHEN MOD(rn,20)=0 THEN 'failed' WHEN MOD(rn,50)=0 THEN 'chargeback'
         WHEN MOD(rn,30)=0 THEN 'refunded' ELSE 'authorized' END,
    CASE WHEN MOD(rn,20)=0 THEN 'insufficient_funds'
         WHEN MOD(rn,40)=0 THEN 'card_declined' ELSE NULL END,
    'DEVICE_' || LPAD(MOD(rn,10000),5,'0'),
    CASE MOD(rn,3) WHEN 0 THEN 'mobile' WHEN 1 THEN 'desktop' ELSE 'tablet' END,
    '192.168.' || MOD(rn,255) || '.' || MOD(rn*3,255),
    CASE MOD(rn,10) WHEN 0 THEN 'GB' WHEN 1 THEN 'CA' WHEN 2 THEN 'AU'
        WHEN 3 THEN 'DE' WHEN 4 THEN 'FR' ELSE 'US' END,
    CASE MOD(rn,5) WHEN 0 THEN 'London' WHEN 1 THEN 'Toronto'
        WHEN 2 THEN 'Sydney' WHEN 3 THEN 'Berlin' ELSE 'New York' END,
    CASE MOD(rn,5) WHEN 0 THEN 'retail' WHEN 1 THEN 'travel'
        WHEN 2 THEN 'food' WHEN 3 THEN 'entertainment' ELSE 'ecommerce' END,
    CASE WHEN MOD(rn,10) IN (0,1,2,3,4) THEN FALSE ELSE TRUE END,
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- CLICKSTREAM (2M)
INSERT INTO RAW.CLICKSTREAM
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 2000000))
)
SELECT
    'EVT_' || LPAD(rn,9,'0'),
    'SESS_' || LPAD(MOD(rn,200000),7,'0'),
    CASE WHEN MOD(rn,5)=0 THEN NULL
         ELSE 'CUST_' || LPAD(MOD(rn,50000)+1,6,'0') END,
    'ANON_' || LPAD(MOD(rn,100000),6,'0'),
    DATEADD('second', -UNIFORM(1,31536000,RANDOM()), CURRENT_TIMESTAMP()),
    CASE MOD(rn,8) WHEN 0 THEN 'page_view' WHEN 1 THEN 'product_view'
        WHEN 2 THEN 'add_to_cart' WHEN 3 THEN 'search'
        WHEN 4 THEN 'page_view' WHEN 5 THEN 'product_view'
        WHEN 6 THEN 'checkout_start' ELSE 'purchase' END,
    '/page/' || MOD(rn,100),
    CASE WHEN MOD(rn,3)=0 THEN 'PROD_' || LPAD(MOD(rn,500)+1,4,'0') ELSE NULL END,
    CASE MOD(rn,6) WHEN 0 THEN 'electronics' WHEN 1 THEN 'apparel'
        WHEN 2 THEN 'home' WHEN 3 THEN 'sports'
        WHEN 4 THEN 'beauty' ELSE 'accessories' END,
    CASE WHEN MOD(rn,8)=3 THEN 'search term ' || MOD(rn,50) ELSE NULL END,
    CASE MOD(rn,3) WHEN 0 THEN 'mobile' WHEN 1 THEN 'desktop' ELSE 'tablet' END,
    CASE MOD(rn,2) WHEN 0 THEN 'web' ELSE 'mobile_app' END,
    CASE WHEN MOD(rn,5)=0 THEN 'google.com'
         WHEN MOD(rn,7)=0 THEN 'facebook.com' ELSE NULL END,
    UNIFORM(5,300,RANDOM()),
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- CAMPAIGNS (200K)
INSERT INTO RAW.CAMPAIGNS
WITH gen AS (
    SELECT ROW_NUMBER() OVER (ORDER BY SEQ4()) AS rn
    FROM TABLE(GENERATOR(ROWCOUNT => 200000))
)
SELECT
    'CAMP_RESP_' || LPAD(rn,8,'0'),
    'CUST_' || LPAD(MOD(rn,50000)+1,6,'0'),
    'CAMP_' || LPAD(MOD(rn,50)+1,3,'0'),
    'Campaign ' || MOD(rn,50),
    CASE MOD(rn,5) WHEN 0 THEN 'email' WHEN 1 THEN 'push'
        WHEN 2 THEN 'sms' WHEN 3 THEN 'display' ELSE 'paid_social' END,
    DATEADD('second', -UNIFORM(1,7776000,RANDOM()), CURRENT_TIMESTAMP()),
    CASE WHEN MOD(rn,3)=0
         THEN DATEADD('hour', UNIFORM(1,48,RANDOM()), CURRENT_TIMESTAMP()) ELSE NULL END,
    CASE WHEN MOD(rn,6)=0
         THEN DATEADD('hour', UNIFORM(1,72,RANDOM()), CURRENT_TIMESTAMP()) ELSE NULL END,
    CASE WHEN MOD(rn,15)=0
         THEN DATEADD('hour', UNIFORM(1,96,RANDOM()), CURRENT_TIMESTAMP()) ELSE NULL END,
    CASE WHEN MOD(rn,25)=0
         THEN DATEADD('day', UNIFORM(1,30,RANDOM()), CURRENT_TIMESTAMP()) ELSE NULL END,
    CASE WHEN MOD(rn,15)=0
         THEN ROUND(UNIFORM(10,300,RANDOM())::FLOAT,2) ELSE NULL END,
    'synthetic_v1',
    CURRENT_TIMESTAMP()
FROM gen;

-- Verify
SELECT 'CUSTOMERS'  AS tbl, COUNT(*) AS rows FROM RAW.CUSTOMERS   UNION ALL
SELECT 'PRODUCTS',          COUNT(*)          FROM RAW.PRODUCTS    UNION ALL
SELECT 'ORDERS',            COUNT(*)          FROM RAW.ORDERS      UNION ALL
SELECT 'ORDER_ITEMS',       COUNT(*)          FROM RAW.ORDER_ITEMS UNION ALL
SELECT 'TRANSACTIONS',      COUNT(*)          FROM RAW.TRANSACTIONS UNION ALL
SELECT 'CLICKSTREAM',       COUNT(*)          FROM RAW.CLICKSTREAM UNION ALL
SELECT 'CAMPAIGNS',         COUNT(*)          FROM RAW.CAMPAIGNS
ORDER BY 1;