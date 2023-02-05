-- create income statement fact table
drop table if exists ticker_data.fact_income_statement;
CREATE TABLE ticker_data.fact_income_statement 
(
symbol_id bigint
, date_id bigint
, symbol text
, date_actual date 
, reported_currency text
, cik text
, filling_date text
, accepted_date text
, revenue bigint
, cost_of_revenue bigint
, gross_profit bigint
, gross_profit_ratio double precision
, research_and_development_expenses bigint
, general_and_administrative_expenses bigint
, selling_and_marketing_expenses bigint
, selling_general_and_administrative_expenses bigint
, other_expenses bigint
, operating_expenses bigint
, cost_and_expenses bigint
, interest_income bigint
, interest_expense bigint
, depreciation_and_amortization bigint
, ebitda bigint
, ebitdaratio double precision
, operating_income bigint
, operating_income_ratio double precision
, total_other_income_expenses_net bigint
, income_before_tax bigint
, income_before_tax_ratio double precision
, income_tax_expense bigint
, net_income bigint
, net_income_ratio double precision
, eps double precision
, epsdiluted double precision
, weighted_average_shs_out bigint
, weighted_average_shs_out_dil bigint
, link text
, final_link text
,  PRIMARY KEY (symbol_id, date_id)
,  FOREIGN KEY (symbol_id) REFERENCES ticker_data.all_symbols(id)
);

-- create balance sheet fact table
drop table if exists ticker_data.fact_balance_sheet;
CREATE TABLE ticker_data.fact_balance_sheet 
(
symbol_id bigint
, date_id bigint
, symbol text
, date_actual date 
, reported_currency text
, cik text
, filling_date text
, accepted_date text
, cash_and_cash_equivalents bigint
, short_term_investments bigint
, cash_and_short_term_investments bigint
, net_receivables bigint
, inventory bigint
, other_current_assets bigint
, total_current_assets bigint
, property_plant_equipment_net bigint
, goodwill bigint
, intangible_assets bigint
, goodwill_and_intangible_assets bigint
, long_term_investments bigint
, tax_assets bigint
, other_non_current_assets bigint
, total_non_current_assets bigint
, other_assets bigint
, total_assets bigint
, account_payables bigint
, short_term_debt bigint
, tax_payables bigint
, deferred_revenue bigint
, other_current_liabilities bigint
, total_current_liabilities bigint
, long_term_debt bigint
, deferred_revenue_non_current bigint
, deferred_tax_liabilities_non_current bigint
, other_non_current_liabilities bigint
, total_non_current_liabilities bigint
, other_liabilities bigint
, capital_lease_obligations bigint
, total_liabilities bigint
, preferred_stock bigint
, common_stock bigint
, retained_earnings bigint
, accumulated_other_comprehensive_income_loss bigint
, othertotal_stockholders_equity bigint
, total_stockholders_equity bigint
, total_equity bigint
, total_liabilities_and_stockholders_equity bigint
, minority_interest bigint
, total_liabilities_and_total_equity bigint
, total_investments bigint
, total_debt bigint
, net_debt bigint
, link text
, final_link text
,  PRIMARY KEY (symbol_id, date_id)
,  FOREIGN KEY (symbol_id) REFERENCES ticker_data.all_symbols(id)
);

-- create cash flow statement fact table
drop table if exists ticker_data.fact_cash_flow_statement;
CREATE TABLE ticker_data.fact_cash_flow_statement 
(
symbol_id bigint
, date_id bigint
, symbol text
, date_actual date 
, reported_currency text
, cik text
, filling_date text
, accepted_date text
, net_income bigint
, depreciation_and_amortization bigint
, deferred_income_tax bigint
, stock_based_compensation bigint
, change_in_working_capital bigint
, accounts_receivables bigint
, inventory bigint
, accounts_payables bigint
, other_working_capital bigint
, other_non_cash_items bigint
, net_cash_provided_by_operating_activities bigint
, investments_in_property_plant_and_equipment bigint
, acquisitions_net bigint, purchases_of_investments bigint
, sales_maturities_of_investments bigint
, other_investing_activites bigint
, net_cash_used_for_investing_activites bigint
, debt_repayment bigint
, common_stock_issued bigint
, common_stock_repurchased bigint
, dividends_paid bigint
, other_financing_activites bigint
, net_cash_used_provided_by_financing_activities bigint
, effect_of_forex_changes_on_cash bigint
, net_change_in_cash bigint
, cash_at_end_of_period bigint
, cash_at_beginning_of_period bigint
, operating_cash_flow bigint
, capital_expenditure bigint
, free_cash_flow bigint
, link text
, final_link text
,  PRIMARY KEY (symbol_id, date_id)
,  FOREIGN KEY (symbol_id) REFERENCES ticker_data.all_symbols(id)
);

-- Create date dimension table 
DROP TABLE if exists dim_date;
CREATE TABLE dim_date
(
  date_dim_id              INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE public.dim_date ADD CONSTRAINT dim_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

CREATE INDEX dim_date_date_actual_idx
  ON dim_date(date_actual);

COMMIT;

INSERT INTO dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_dim_id,
       datum AS date_actual,
       EXTRACT(EPOCH FROM datum) AS epoch,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(ISODOW FROM datum) AS day_of_week,
       EXTRACT(DAY FROM datum) AS day_of_month,
       datum - DATE_TRUNC('quarter', datum)::DATE + 1 AS day_of_quarter,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW-') || EXTRACT(ISODOW FROM datum) AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE
           ELSE FALSE
           END AS weekend_indr
FROM (SELECT '1970-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

COMMIT;