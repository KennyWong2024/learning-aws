CREATE TABLE "group_one"."project_bronze"."raw_sample_submission" (
    id BIGINT,
    sales BIGINT
);

CREATE TABLE "group_one"."project_bronze"."raw_store"(
  store BIGINT, 
  storetype VARCHAR(100), 
  assortment VARCHAR(100), 
  competitiondistance BIGINT, 
  competitionopensincemonth BIGINT, 
  competitionopensinceyear BIGINT, 
  promo2 BIGINT, 
  promo2sinceweek BIGINT, 
  promo2sinceyear BIGINT, 
  promointerval VARCHAR(100)
);

CREATE TABLE "group_one"."project_bronze"."raw_test" (
  id BIGINT, 
  store BIGINT, 
  dayofweek BIGINT, 
  date DATE, 
  is_open VARCHAR(50),
  promo BIGINT, 
  stateholiday VARCHAR(50),
  schoolholiday VARCHAR(50) 
);

CREATE TABLE "group_one"."project_bronze"."raw_train" (
  store BIGINT, 
  dayofweek BIGINT, 
  date DATE, 
  sales BIGINT, 
  customers BIGINT, 
  is_open VARCHAR(50),
  promo BIGINT, 
  stateholiday VARCHAR(50),
  schoolholiday VARCHAR(50)
);