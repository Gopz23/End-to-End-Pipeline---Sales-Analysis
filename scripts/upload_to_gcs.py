def upload_file(bucket_name, source_file, destination_blob):
    try:
        client = storage.Client(project="gopz-sales-pipeline-2026")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)

        blob.upload_from_filename(source_file)

        logging.info(f"✅ Uploaded {source_file} to gs://{bucket_name}/{destination_blob}")

    except Exception as e:
        logging.error(f"❌ Upload failed: {str(e)}")
