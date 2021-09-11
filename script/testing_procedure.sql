-- PART_NUMBER = ITEM + _ + CCY
-- COST_CENTER = LEFT(ITEM,5)
-- BUDGET = IFF(CATEGORY like 'SPARE%', 10000, 2000)
-- TABLE_NAME = IFF(TABLE like 'tbl_products', 'tbl_products', 'tbl_spare_parts')
-- Prioritizing data from TBL_SPARE_PARTS when duplicates comes from both tables

CREATE OR REPLACE PROCEDURE "DB_NAME"."MY_SCHEMA".GET_ITEMS_DETAILS()
RETURNS string not null
LANGUAGE javascript
AS
$$

var tbl_items_master = '"DB_NAME"."MY_SCHEMA"."TBL_ITEMS_MASTER"';

  var tbl_products = '"DB_NAME"."MY_SCHEMA"."TBL_PRODUCTS"';
  var tbl_spare_parts = '"DB_NAME"."MY_SCHEMA"."TBL_SPARE_PARTS"'; // spare parts have a priority

  var insert_tex = "";
  insert_text += "INSERT INTO " + tbl_items_master + " (ITEM, PART_NUMBER, BUDGET, COST_CENTER, TABLE_NAME, USER_ID, EDIT_TIMESTAMP) ";
  insert_text += "SELECT DISTINCT i.ITEM, ";
  insert_text += "CONCAT(i.ITEM, '_', i.CCY), "
  insert_text += "IFF(i.CATEGORY LIKE 'SPARE%', 10000, 2000), ";
  insert_text += "IFF(i.TABLE LIKE 'tbl_products%', 'tbl_products', 'tbl_spare_parts'), "
  insert_text += "'system', ";
  insert_text += 'current_timestamp()' + " ";

  var inner_query += "";
  inner_query += "SELECT *, COUNT(*) OVER(PARTITION BY ITEM ORDER BY ITEM) ITEM_COUNT FROM ("
  inner_query += "SELECT ITEM, CATEGORY, CCY, DATA_SRC, DATA_SRC_FILENAME, 'TBL_PRODUCTS' AS TABLE FROM " + tbl_products + " ";
  inner_query += "UNION ";
  inner_query += "SELECT ITEM, 'Spare', CCY, DATA_SRC, DATA_SRC_FILENAME, 'TBL_SPARE_PARTS' AS TABLE FROM " + tbl_spare_parts + ") ";
  inner_query += "WHERE ITEM_COUNT = 1 OR (ITEM_COUNT > 1 AND TABLE = 'TBL_SPARE_PARTS') "

  sql_insert = insert_tex + "FROM (" + inner_query + ") AS i;";

  var sql_command = snowflake.createStatement({sqlText: sql_insert});

  try {
      var sql_result = sql_command.execute();
  }
  catch (e) {
      return 'creating temporary table wiht unique ITEM failed: ' + e;
  }

  return 'Done';

$$;

# Scenario I - testing creation of PART_NUMBER
# TRUE when PART_NUMBER == 'Item_test1_EUR'

INSERT INTO "MY_DB"."MY_SCHEMA"."TBL_PRODUCTS" (ITEM, CCY, DATA_SRC, DATA_SRC_FILENAME)
VALUES ('12345_ITEM_TEST_1', 'EUR', 'COMPANY_SYSTEM', 'comapny_system_data.csv');

CALL "MY_DB"."MY_SCHEMA".GET_ITEMS_DETAILS();

SELECT 'Item_test1_EUR' = (
  SELECT PART_NUMBER
  FROM "MY_DB"."MY_SCHEMA"."TBL_ITEMS_MASTER"
  WHERE ITEM = '12345_ITEM_TEST_1';
)


# Scenario II - testing creation of COST_CENTER
# TRUE when COST_CENTER == '989898'

INSERT INTO "MY_DB"."MY_SCHEMA"."TBL_PRODUCTS" (ITEM, CCY, DATA_SRC, DATA_SRC_FILENAME)
VALUES ('989898_ITEM_TEST_2', 'EUR', 'COMPANY_SYSTEM', 'comapny_system_data.csv');

CALL "MY_DB"."MY_SCHEMA".GET_ITEMS_DETAILS();

SELECT '989898' = (
  SELECT COST_CENTER
  FROM "MY_DB"."MY_SCHEMA"."TBL_ITEMS_MASTER"
  WHERE ITEM = '989898_ITEM_TEST_2';
)


# Scenario III - proritizing duplicates from TBL_SPARE_PARTS and removing ones form TBL_PRODUCTS
# TRUE when TABLE_NAME == 'TBL_SPARE_PARTS' for '11111_ITEM_TEST_3'

INSERT INTO "MY_DB"."MY_SCHEMA"."TBL_SPARE_PARTS" (ITEM, CCY, DATA_SRC, DATA_SRC_FILENAME)
VALUES ('11111_ITEM_TEST_3', 'USD', 'COMPANY_SYSTEM', 'comapny_system_data.csv')

INSERT INTO "MY_DB"."MY_SCHEMA"."TBL_PRODUCTS" (ITEM, CCY, CATEGORY, DATA_SRC, DATA_SRC_FILENAME)
VALUES ('11111_ITEM_TEST_3', 'EUR', 'NEW', 'ERP_1', 'erp_data.csv')

CALL "MY_DB"."MY_SCHEMA".GET_ITEMS_DETAILS()

SELECT 'TBL_SPARE_PARTS' = (
  SELECT TABLE_NAME
  FROM "MY_DB"."MY_SCHEMA"."TBL_ITEMS_MASTER"
  WHERE ITEM = '11111_ITEM_TEST_3';
)
