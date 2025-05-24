### Each table must be created one by one

CREATE TABLE HW2_RN.PUBLIC.FACT_BUS (
	NUMBER_OF_STUDENTS_ON_THE_BUS VARCHAR(667036),
	HAS_CONTRACTOR_NOTIFIED_SCHOOLS BOOLEAN,
	HAS_CONTRACTOR_NOTIFIED_PARENTS BOOLEAN,
	HAVE_YOU_ALERTED_OPT BOOLEAN,
	BREAKDOWN BOOLEAN,
	PRE_K BOOLEAN,
	HOW_LONG_DELAYED_IN_MIN VARCHAR(667036),
	UNIQUE_ID NUMBER(38,0),
	ROUTE_ID NUMBER(38,0),
	FACTS_ID NUMBER(38,0),
	BUS_ID NUMBER(38,0)
);

CREATE TABLE dim_date(
    occurred_on_quarter INT,
    occurred_on_month INT,
    occurred_on_day INT,
    occurred_on_hour INT,
    occurred_on_day_of_week INT,
    occurred_on_week_of_month INT,
    week_of_year INT,
    occurred_on_holiday BOOLEAN,
    occurred_on_weekend BOOLEAN,
    occurred_on_month_name STRING,
    unique_id INT
);

CREATE TABLE dim_route(
    route_number STRING,
    route_id INT
);

CREATE TABLE dim_incident(
    run_type STRING,
    reason STRING,
    incident_number INT
);

CREATE TABLE dim_bus_info(
    bus_no STRING,
    bus_id INT,
    bus_company_name STRING,
    schools_serviced STRING
);