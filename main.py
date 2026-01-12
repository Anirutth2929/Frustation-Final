import os
import csv
import io
import pymysql
from google.cloud import storage


def main():
    # ---------- Config from environment variables ----------
    bucket_name = os.environ["BUCKET_NAME"]
    output_file = os.environ["OUTPUT_FILE_NAME"]
    db_name = os.environ["DB_NAME"]
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]

    # ---------- DB credentials from environment variables ----------
    db_user = os.environ["DB_USER"]
    db_password = os.environ["DB_PASSWORD"]

    # ---------- Connect to Cloud SQL (MySQL) ----------
    connection = pymysql.connect(
        user=db_user,
        password=db_password,
        unix_socket=f"/cloudsql/{instance_connection_name}",
        database=db_name,
    )

    cursor = connection.cursor()
    cursor.execute("SELECT id, name, email FROM users")
    rows = cursor.fetchall()

    # ---------- Create CSV in memory ----------
    buffer = io.StringIO()
    writer = csv.writer(buffer)

    writer.writerow(["id", "name", "email"])
    for row in rows:
        writer.writerow(row)

    # ---------- Upload CSV to GCS ----------
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(output_file)

    blob.upload_from_string(
        buffer.getvalue(),
        content_type="text/csv"
    )

    cursor.close()
    connection.close()

    print(f"Exported {len(rows)} rows to gs://{bucket_name}/{output_file}")


if __name__ == "__main__":
    main()
