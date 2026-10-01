import pandas as pd
import io

def calculate_period_ms(period):
    if period is None:
        return None

    unit = period[-1]        # last character: 'm', 'h', 'd'
    value = int(period[:-1]) # everything before it: '15', '1', '4'

    unit_ms = {
        'm': 60 * 1000,           # minute
        'h': 60 * 60 * 1000,      # hour
        'd': 24 * 60 * 60 * 1000, # day
    }
    return value * unit_ms[unit]

def upsert_to_s3(s3, bucket, key, new_df, time_field):
    """
    Merge new_df into the Parquet file at key 
    (newest wins on duplicate time_field) and write it back.
    """
    # REVIEW #4: read -> modify -> write is not atomic. Two concurrent Lambda runs both read the
    # same file and the last writer wipes the other's rows (lost update). Fix is in terraform/lambda.tf.
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        existing = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        combined = pd.concat([existing, new_df])
        combined = combined.drop_duplicates(subset=time_field, keep="last").sort_values(time_field)

    except s3.exceptions.NoSuchKey:
        combined = new_df  # First write — no existing file

    buf = io.BytesIO()
    combined.to_parquet(buf, engine="pyarrow", index=False)
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    return len(combined)
