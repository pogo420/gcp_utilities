//// creating table 
//use schema BQ_SNOWFLAKE_DEMO.public;
//create or replace table BQ_SNOWFLAKE_DEMO.public.stg_demo_data
//(
//invoice_and_item_number	STRING	,
//date	DATE	,
//store_number	STRING	,
//store_name	STRING	,
//address	STRING	,
//city	STRING	,
//zip_code	STRING	,
//store_location	STRING	,
//county_number	STRING	,
//county	STRING	,
//category	STRING	,
//category_name	STRING	,
//vendor_number	STRING	,
//vendor_name	STRING	,
//item_number	STRING	,
//item_description	STRING	,
//pack	INTEGER	,
//bottle_volume_ml	INTEGER	,
//state_bottle_cost	FLOAT	,
//state_bottle_retail	FLOAT	,
//bottles_sold	INTEGER	,
//sale_dollars	FLOAT	,
//volume_sold_liters	FLOAT	,
//volume_sold_gallons	FLOAT	
//);
//

//truncate table BQ_SNOWFLAKE_DEMO.public.stg_demo_data;
//truncate table BQ_SNOWFLAKE_DEMO.public.demo_data;

//select * from BQ_SNOWFLAKE_DEMO.public.stg_demo_data;

MERGE into 
    BQ_SNOWFLAKE_DEMO.public.demo_data trg
using 
    BQ_SNOWFLAKE_DEMO.public.stg_demo_data src -- truncate load 
on
    trg.INVOICE_AND_ITEM_NUMBER = src.INVOICE_AND_ITEM_NUMBER
    and trg.date = src.date
    and trg.store_number = src.store_number
when matched
    and 
    (trg.store_name<>src.store_name
        or trg.address<>src.address
        or trg.city<>src.city
        or trg.zip_code<>src.zip_code
        or trg.store_location<>src.store_location
        or trg.county_number<>src.county_number
        or trg.county<>src.county
        or trg.category<>src.category
        or trg.category_name<>src.category_name
        or trg.vendor_number<>src.vendor_number
        or trg.vendor_name<>src.vendor_name
        or trg.item_number<>src.item_number
        or trg.item_description<>src.item_description
        or trg.pack<>src.pack
        or trg.bottle_volume_ml<>src.bottle_volume_ml
        or trg.state_bottle_cost<>src.state_bottle_cost
        or trg.state_bottle_retail<>src.state_bottle_retail
        or trg.bottles_sold<>src.bottles_sold
        or trg.sale_dollars<>src.sale_dollars
        or trg.volume_sold_liters<>src.volume_sold_liters
        or trg.volume_sold_gallons<>src.volume_sold_gallons
     )
then update set
    trg.store_name=src.store_name,
    trg.address=src.address,
    trg.city=src.city,
    trg.zip_code=src.zip_code,
    trg.store_location=src.store_location,
    trg.county_number=src.county_number,
    trg.county=src.county,
    trg.category=src.category,
    trg.category_name=src.category_name,
    trg.vendor_number=src.vendor_number,
    trg.vendor_name=src.vendor_name,
    trg.item_number=src.item_number,
    trg.item_description=src.item_description,
    trg.pack=src.pack,
    trg.bottle_volume_ml=src.bottle_volume_ml,
    trg.state_bottle_cost=src.state_bottle_cost,
    trg.state_bottle_retail=src.state_bottle_retail,
    trg.bottles_sold=src.bottles_sold,
    trg.sale_dollars=src.sale_dollars,
    trg.volume_sold_liters=src.volume_sold_liters,
    trg.volume_sold_gallons=src.volume_sold_gallons
    
when not matched then insert
    values(
      src.invoice_and_item_number,
      src.date,
      src.store_number,
      src.store_name,
      src.address,
      src.city,
      src.zip_code,
      src.store_location,
      src.county_number,
      src.county,
      src.category,
      src.category_name,
      src.vendor_number,
      src.vendor_name,
      src.item_number,
      src.item_description,
      src.pack,
      src.bottle_volume_ml,
      src.state_bottle_cost,
      src.state_bottle_retail,
      src.bottles_sold,
      src.sale_dollars,
      src.volume_sold_liters,
      src.volume_sold_gallons
    )
;

select * from BQ_SNOWFLAKE_DEMO.public.demo_data
where 
 date = '2019-01-23'
  AND store_number = '5676'
  AND store_name = 'Family Pantry'
  ;