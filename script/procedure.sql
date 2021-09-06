CREATE OR REPLACE PROCEDURE "db_name"."schema_name".prcedure_name()
RETURNS string
LANGUAGE javascript
AS
$$

// Creating 2 temporary tables:
// 1st one contains all ITEM with a single category assigned (there may be duplicates)
// 2nd one contains unique ITEM with order_status defined

//  Creating temporary table with all categories per ITEM

    var tbl_products = '"db_name"."schema_name"."tbl_products"';
    var tbl_tools = '"db_name"."schema_name"."tbl_tools"';
    var tbl_items = '"db_name"."schema_name"."tbl_items"';
    var tbl_things = '"db_name"."schema_name"."tbl_things"';
    var tbl_spare_parts = '"db_name"."schema_name"."tbl_spare_parts"';

    var tbl_temporary_name = '"db_name"."schema_name"."tbl_all_stuff"';

    var tbl_temporary_definition = "";
    tbl_temporary_definition += "CREATE OR REPLACE TABLE " + tbl_temporary_name + " AS ";
    tbl_temporary_definition += "SELECT *, SUM(CASE WHEN CATEGORY = 'Spare' THEN 1 ELSE 0 END) OVER(PARTITION BY ITEM ORDER BY ITEM) AS IF_SPARE_SOMEWHERE, COUNT(*) OVER(PARTITION BY ITEM ORDER BY ITEM) ITEM_COUNT FROM("
    tbl_temporary_definition += "SELECT ITEM, CATEGORY, PART_NUMBER, DATA_SRC, DATA_SRC_FILENAME FROM " + tbl_products + " ";
    tbl_temporary_definition += "UNION ";
    tbl_temporary_definition += "SELECT ITEM, CATEGORY, PART_NUMBER, DATA_SRC, DATA_SRC_FILENAME FROM " + tbl_tools + " ";
    tbl_temporary_definition += "UNION ";
    tbl_temporary_definition += "SELECT ITEM, CATEGORY, PART_NUMBER, DATA_SRC, DATA_SRC_FILENAME FROM " + tbl_items + " ";
    tbl_temporary_definition += "UNION ";
    tbl_temporary_definition += "SELECT ITEM, CATEGORY, PART_NUMBER, DATA_SRC, DATA_SRC_FILENAME FROM " + tbl_things + " ";
    tbl_temporary_definition += "UNION ";
    tbl_temporary_definition += "SELECT ITEM, 'Spare', PART_NUMBER, DATA_SRC, DATA_SRC_FILENAME FROM " + tbl_spare_parts;
    tbl_temporary_definition += ") ORDER BY 1;";

    // example
    // ------------------------------------------------------------------------------
    // ITEM |   CATEGORY      	| ... | DATA_SRC    | ... |   IF_SPARE_SOMEWHERE   	|
    // ------------------------------------------------------------------------------
    // 111  |   Spare           | ... | ERP         | ... |   0                     |
    // ------------------------------------------------------------------------------
    // 123  |   Spare           | ... | ERP         | ... |   0                     |
    // 123  |   different       | ... | ERP         | ... |   0                     |
    // 123  |   different_2     | ... | ERP         | ... |   0                     |
    // ------------------------------------------------------------------------------
    // 999  |   something       | ... | ERP         | ... |   1                     |
    // 999  |   something_2     | ... | ERP         | ... |   1                     |
    // 999  |   Spare           | ... | SSYSTEM     | ... |   1                     |

    var sql_command = snowflake.createStatement({sqlText: tbl_temporary_definition});

    try {
        var sql_result = sql_command.execute();
    }
    catch (e) {
        return 'creating temporary table failed: ' + e;
    }

// deleting duplicated ITEM when coming from both SAP and SUPER_SYSTEM (deleting SAP ones)
// prioritizing SUPER_SYSTEM entires
// due to example - it will remove two records with ITEM of 999 where DATA_SRC is ERP

    var delete_sql_statement = "";
    delete_sql_statement += "DELETE ";
    delete_sql_statement += "FROM " + tbl_temporary_name + " ";
    delete_sql_statement += "WHERE ITEM_COUNT > 1 AND CATEGORY != 'Spare' AND IF_SPARE_SOMEWHERE = 1;";

    var sql_command = snowflake.createStatement({sqlText: delete_sql_statement});

    try {
        var sql_result = sql_command.execute();
    }
    catch (e) {
        return 'deleting ERP duplicates from tbl_all_stuff failed: ' + e;
    }

// dropping ITEM_COUNT_COLUMN and IF_CURRIMA_SOMEWHERE from tbl_all_stuff

    var drop_column = "";
    drop_column += "ALTER TABLE " + tbl_temporary_name + " ";
    drop_column += "DROP COLUMN ITEM_COUNT, IF_SPARE_SOMEWHERE;";

    var sql_command = snowflake.createStatement({sqlText: drop_column});

    try {
        var sql_result = sql_command.execute();
    }
    catch (e) {
        return 'droping column from tbl_all_stuff failed: ' + e;
    }

    // example:
    // --------------------------------------------------
    // ITEM   |   CATEGORY	      | ... | DATA_SRC  | ...
    // --------------------------------------------------
    // 111    |   Spare           | ... | ERP       | ...
    // --------------------------------------------------
    // 123    |   Spare           | ... | ERP       | ...
    // 123    |   different       | ... | ERP       | ...
    // 123    |   different_2     | ... | ERP       | ...
    // --------------------------------------------------
    // 999    |   Spare           | ... | SSYSTEM   | ...

//  Insert for testing -> ITEM 666111 should appear in the tbl_items_master at the end of procedure - if so then test passed - omment out when want to skip
//    var insert_text = "INSERT INTO " + tbl_temporary_name +
//	" (ITEM, CATEGORY, PART_NUMBER, DATA_SRC) VALUES('666111', 'Spare', '12345', 'SSYSTEM_B')";
//    var stmt_insertIntoTblProjectsMaster_command = snowflake.createStatement({sqlText: insert_text});
//    var res = stmt_insertIntoTblProjectsMaster_command.execute();

//  Creating table with unique item and order_status

    var tbl_temporary_unique_item_name = '"db_name"."schema_name"."tbl_temp_unique_item"';

    // everything from super_system is open and has higher priority than SAP entieris
    // (sap ones should be dropped when diplicate super_system ones)

    var case_logic = ""
    case_logic += "CASE ";
    case_logic += "WHEN CATEGORY = 'Spare' THEN 'open' ";
    case_logic += "WHEN if_spare >= 1 AND count > 1 THEN 'open' ";
    case_logic += "WHEN if_spare = 1 AND count = 1 THEN 'to be checked' ";
    case_logic += "WHEN if_spare = 0 AND count > 0 THEN 'open' ";
    case_logic += "WHEN if_spare = 0 AND count = 0 THEN 'closed' ";
    case_logic += "END ";

    var tbl_temporary_definition = "";
    tbl_temporary_definition += "CREATE OR REPLACE TABLE " + tbl_temporary_unique_item_name + " AS ";
    tbl_temporary_definition += "SELECT DISTINCT ITEM, ";
    tbl_temporary_definition += "SUM(CASE WHEN CATEGORY = 'Spare' THEN 1 ELSE 0 END) OVER(PARTITION BY ITEM ORDER BY ITEM) AS if_spare, ";
    tbl_temporary_definition += "COUNT(*) OVER(PARTITION BY ITEM ORDER BY ITEM) AS count, ";
    tbl_temporary_definition += case_logic + " AS order_status ";
    tbl_temporary_definition += "FROM " + tbl_temporary_name;

    var sql_command = snowflake.createStatement({sqlText: tbl_temporary_definition});

    try {
        var sql_result = sql_command.execute();
    }
    catch (e) {
        return 'creating temporary table wiht unique ITEM failed: ' + e;
    }

    // example
    // -------------------------------------------------------
    // ITEM  |   if_spare   |   count   |   order_status
    // -------------------------------------------------------
    // 111  |       1       |   1       |   to be checked
    // -------------------------------------------------------
    // 123  |       1       |   3       |   open
    // -------------------------------------------------------
    // 999  |       0       |   1       |   open
    // -------------------------------------------------------

//  Main
    var tbl_items_master = '"db_name"."schema_name"."tbl_items_master"';
    var tbl_erpdata = "ERP ";
    var insert_text = "INSERT INTO " + tbl_items_master + " (ITEM, PART_NUMBER, ORDER_STATUS, ABC, BUDGET, USER_ID, EDIT_TIMESTAMP) ";

    var outer_query = "";
    outer_query += "SELECT DISTINCT erpdata.ITEM, ";
    outer_query += "'none (this record is system-generted based on " + tbl_erpdata + " data import)', ";
    outer_query += "erpdata.ORDER_STATUS, ";

    // logic to define ABC:
    // ERP_A - A100 (based on DATA_SRC)
    // ERP_B - B300 (based on DATA_SRC)
    // SSYSTEM_A - A100 (based on DATA_SRC_FILENAME)
    // SSYSTEM_B - B300 (based on DATA_SRC_FILENAME)
    // else - tbd

    outer_query += "iff(erpdata.DATA_SRC like 'ERP%', iff(erpdata.DATA_SRC like 'ERP_A', 'A100', 'B300'), iff(erpdata.DATA_SRC_FILENAME like 'SSYSTEM%', iff(erpdata.DATA_SRC_FILENAME like '%SSYSTEM_A%', 'A100', 'B300'), 'tbd')), ";
    outer_query += "left(erpdata.PART_NUMBER,5), ";
    outer_query += "'system' user_id, ";
    outer_query += 'current_timestamp()' + " ";

    var inner_query = "";
    inner_query += "SELECT a.*, p.ORDER_STATUS ";
    inner_query += "FROM " + tbl_temporary_name + " a ";
    inner_query += "JOIN " + tbl_temporary_unique_psp_name + " p ";
    inner_query += "ON a.ITEM = p.ITEM";

    // inner_query example result:
    // ---------------------------------------------------------------------------
    // ITEM  | CATEGORY     |   PART_NUMBER  |  DATA_SRC  |  ORDER_STATUS
    // ---------------------------------------------------------------------------
    // 123  | Spare         |        ...        |    ...     |  open
    // ---------------------------------------------------------------------------
    // 123  | different     |        ...        |    ...     |  open
    // ---------------------------------------------------------------------------
    // 123  | different_2   |         ...       |    ...     |  open
    // ---------------------------------------------------------------------------

    var sub_from_text = "FROM (" + inner_query + ") erpdata ";

    var sub_join_text = "FULL OUTER JOIN " + tbl_items_master + " projects on projects.ITEM = erpdata.ITEM ";
    var sub_where_text = "WHERE projects.ITEM is null and erpdata.ITEM is not null;";
    var values_text = outer_query + sub_from_text + sub_join_text + sub_where_text;
    var insertIntoTblProjectsMaster_command = insert_text + values_text;

    var stmt_insertIntoTblProjectsMaster_command = snowflake.createStatement({sqlText: insertIntoTblProjectsMaster_command});

    try {
        var res = stmt_insertIntoTblProjectsMaster_command.execute();
    }
    catch (e) {
        return 'inserting to master table failed: ' + e;
    }

//  Dropping tbl_temp_unique_psp
//  comment these out if you want to keep the table for further investigation
    var drop_definition = "DROP TABLE " + tbl_temporary_unique_psp_name;
    var sql_command = snowflake.createStatement({sqlText: drop_definition});
    var sql_result = sql_command.execute();


//  Dropping temporary table
//  comment these out if you want to keep the table for further investigation
    var drop_definition = "DROP TABLE " + tbl_temporary_name;
    var sql_command = snowflake.createStatement({sqlText: drop_definition});
    var sql_result = sql_command.execute();

    result = "Done"
    return result;

$$;

CALL "db_name"."schema_name".prcedure_name()
