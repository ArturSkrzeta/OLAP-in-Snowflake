create or replace stage "DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."STG_LOAD_USER_NAMES"
  url='s3://ferdi-poc/ad_hoc_data_uploads/20211123_user_names'
  storage_integration = "DB_DEV_ANALYTICS.PS_AN_FERDI_POC.AWS_191229304603_Integration"
  encryption=(type='AWS_SSE_KMS' kms_key_id='$KMS_ENV');

update "DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."T_TBL_ROW_LEVEL_USERS"
    set first_name = names.first_name,
        last_name = names.last_name
    from (
          select stg.$1 gid, stg.$2 last_name, stg.$3 first_name
          from @"DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."STG_LOAD_USER_NAMES"
          (file_format =>"DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."FF_PROJECT_MANUALINPUTS_RAW") stg
         ) names
    where "DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."T_TBL_ROW_LEVEL_USERS".gid = names.gid

drop stage "DB_DEV_ANALYTICS"."PS_AN_FERDI_POC"."STG_LOAD_USER_NAMES";
