def delete_old_files(raw_path, table_name, days_threshold=45):
    try:
        spark = SparkSession.builder.appName("DeleteOldFiles").getOrCreate()
        
        today = datetime.datetime.now()
        log_df = spark.table(table_name)
        
        log_df = log_df.withColumn('create_timestamp', fun.col('create_timestamp').cast('date'))
        log_df = log_df.withColumn('delta_days', fun.datediff(fun.lit(today), fun.col('create_timestamp')))
        
        # Filter out rows where status is already 'deleted'
        log_df = log_df.filter(fun.col('status') != 'deleted')

        old_files_df = log_df.filter(fun.col('delta_days') > days_threshold)

        for row in old_files_df.collect():
            file_name = row['file_name']
            try:
                dbutils.fs.rm(f"{raw_path}/{file_name}", True)
                print(f"Deleted file: {file_name}")
                # Update status to 'deleted' in the table
                spark.sql(f"""
                    UPDATE {table_name}
                    SET status = 'deleted'
                    WHERE file_name = '{file_name}'
                """)
            except Exception as e:
                print(f"File not found: {file_name}")
                # Update status to 'not found' in the table

    except Exception as e:
        print(f"An error occurred: {e}")

        Host=synsoaudb11.oci.syneoshealth.com;Port=1528;ServiceName=SYNPSOAU;User Id=SOACUSTOM;Password=PumpK#1ng;WorkArounds=536870912;


uma.chidambaram@syneoshealth.com,surammahindra.reddy@syneoshealth.com,bharat.rayudi@syneoshealth.com,rakshithakeerthi.b@syneoshealth.com,sarvashri.choudhari@syneoshealth.com,pratik.patil@syneoshealth.com,trupti.suryavanshi@syneoshealth.com