DELETE FROM schema 
WHERE field_date = '{{ ti.xcom_pull(key="process_date") }}';