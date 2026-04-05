from google.cloud import storage

def upload_file(bucket_name, source_file, destination_blob):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)

    blob.upload_from_filename(source_file)
    print(f"Uploaded {source_file} to {destination_blob}")

if __name__ == "__main__":
    upload_file(
        "your-bucket-name",
        "../data/sales_data.csv",
        "sales_data.csv"
    )
