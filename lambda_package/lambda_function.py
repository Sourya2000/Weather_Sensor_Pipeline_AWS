import os 
import json
import boto3
import pandas as pd
import psycopg2 
from psycopg2.extras import RealDictCursor 
from io import StringIO
import logging
import traceback
import time
import urllib.parse
import socket

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# Database configuration
DB_HOST = os.environ.get('DB_HOST', 'sensor-db.cv060w0ciem0.eu-north-1.rds.amazonaws.com')
DB_NAME = os.environ.get('DB_NAME', 'postgres')
DB_USER = os.environ.get('DB_USER', 'postgres')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'sensor2026')

# Fix SQS URL - remove quotes if present
raw_sqs_url = os.environ.get('SQS_QUEUE_URL', '')
if raw_sqs_url.startswith('"') and raw_sqs_url.endswith('"'):
    SQS_QUEUE_URL = raw_sqs_url[1:-1]  # Remove surrounding quotes
else:
    SQS_QUEUE_URL = raw_sqs_url

logger.info(f"Configured SQS URL: {SQS_QUEUE_URL}")

def get_db_connection(max_retries=3):
    """Connect to the PostgreSQL database with retries"""
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=30,  # Increased timeout
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5
            )
            logger.info("Database connection successful")
            return conn
        except psycopg2.OperationalError as e:
            retries += 1
            logger.warning(f"Connection attempt {retries} failed: {str(e)}")
            if retries < max_retries:
                wait_time = retries * 5
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_retries} connection attempts failed")
                
                # Try to diagnose the issue
                try:
                    ip = socket.gethostbyname(DB_HOST)
                    logger.info(f"DNS Resolution: {DB_HOST} -> {ip}")
                    
                    # Test TCP connection
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((ip, 5432))
                    if result == 0:
                        logger.info("Port 5432 is reachable but PostgreSQL is not responding")
                    else:
                        logger.error(f"Port 5432 is not reachable (error: {result})")
                    sock.close()
                except Exception as dns_err:
                    logger.error(f"DNS resolution failed: {str(dns_err)}")
                
                raise
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            raise

def validate_row(row):
    """
    Check if a single row of data is valid
    Returns: list_of_errors (empty list if valid)
    """
    errors = []
    
    # Extract values using exact CSV column names
    formatted_date = row.get('Formatted Date')
    temperature = row.get('Temperature (C)')
    humidity = row.get('Humidity')
    
    # Check 1: Required fields can't be empty
    if pd.isna(formatted_date) or formatted_date is None or str(formatted_date).strip() == '':
        errors.append("Missing or empty timestamp")
    if pd.isna(temperature) or temperature is None:
        errors.append("Missing temperature")
    if pd.isna(humidity) or humidity is None:
        errors.append("Missing humidity")
    
    # Check 2: Temperature must be between -50 and 50°C
    if temperature is not None and not pd.isna(temperature):
        try:
            temp = float(temperature)
            if temp < -50 or temp > 50:
                errors.append(f"Temperature out of range: {temp}°C")
        except (ValueError, TypeError):
            errors.append(f"Invalid temperature value: {temperature}")
    
    # Check 3: Humidity must be between 0 and 1
    if humidity is not None and not pd.isna(humidity):
        try:
            hum = float(humidity)
            if hum < 0 or hum > 1:
                errors.append(f"Humidity out of range: {hum}")
        except (ValueError, TypeError):
            errors.append(f"Invalid humidity value: {humidity}")
    
    return errors

def lambda_handler(event, context):
    """
    This is the main function that AWS Lambda calls
    """
    logger.info("⚡ Lambda function started")
    logger.info(f"Event: {json.dumps(event)}")
    
    # Process each file in the event
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        # URL decode the key to handle special characters like + and spaces
        key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        logger.info(f"📥 Processing file: s3://{bucket}/{key}")
        
        try:
            # STEP 1: Check if file exists first
            try:
                # Just check if file exists without downloading yet
                s3.head_object(Bucket=bucket, Key=key)
                logger.info(f"✅ File exists in S3, proceeding to download")
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    logger.error(f"❌ File {key} does not exist in S3 at all!")
                    # List files in incoming folder to see what's there
                    response = s3.list_objects_v2(
                        Bucket=bucket,
                        Prefix='incoming/',
                        MaxKeys=5
                    )
                    if 'Contents' in response:
                        logger.info(f"Files currently in incoming/: {[obj['Key'] for obj in response['Contents']]}")
                    else:
                        logger.info("No files in incoming/ folder")
                else:
                    logger.error(f"Error checking file: {str(e)}")
                continue
            
            # STEP 2: Download the file from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            logger.info(f"✅ Successfully downloaded file from S3")
            
            # STEP 3: Parse CSV into pandas DataFrame
            df = pd.read_csv(StringIO(content))
            logger.info(f"📊 File has {len(df)} rows")
            logger.info(f"📋 CSV columns: {list(df.columns)}")
            
            # Show first row as sample
            if len(df) > 0:
                logger.info(f"🔍 First row sample: {df.iloc[0].to_dict()}")
            
            valid_rows = []
            invalid_rows = []
            
            # STEP 4: Validate each row
            for idx, row in df.iterrows():
                errors = validate_row(row)
                if not errors:
                    valid_rows.append(row)
                else:
                    invalid_rows.append({
                        'row_number': idx + 2,  # +2 for header and 0-index
                        'row': row.to_dict(),
                        'errors': errors
                    })
            
            logger.info(f"✅ Valid rows: {len(valid_rows)}")
            logger.info(f"❌ Invalid rows: {len(invalid_rows)}")
            
            # STEP 5: Handle invalid data (quarantine)
            if invalid_rows:
                quarantine_key = f"quarantine/{key.split('/')[-1]}"
                quarantine_content = json.dumps(invalid_rows, indent=2, default=str)
                s3.put_object(
                    Bucket=bucket,
                    Key=quarantine_key,
                    Body=quarantine_content
                )
                logger.info(f"📦 Invalid data saved to: s3://{bucket}/{quarantine_key}")
                
                # Log to database
                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        "INSERT INTO quarantine_log (file_name, error_reason, raw_data) VALUES (%s, %s, %s)",
                        (key, f"{len(invalid_rows)} invalid rows", json.dumps(invalid_rows[:5], default=str))
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as db_err:
                    logger.error(f"Database logging failed: {str(db_err)}")
            
            # STEP 6: Process valid data
            rows_inserted = 0
            if len(valid_rows) > 0:
                valid_df = pd.DataFrame(valid_rows)
                
                # Calculate aggregates
                aggregates = {
                    'source_file': key,
                    'sensor_type': 'temperature_humidity',
                    'min_temperature': float(valid_df['Temperature (C)'].min()),
                    'max_temperature': float(valid_df['Temperature (C)'].max()),
                    'avg_temperature': float(valid_df['Temperature (C)'].mean()),
                    'stddev_temperature': float(valid_df['Temperature (C)'].std()) if len(valid_df) > 1 else 0,
                    'min_humidity': float(valid_df['Humidity'].min()),
                    'max_humidity': float(valid_df['Humidity'].max()),
                    'avg_humidity': float(valid_df['Humidity'].mean()),
                    'record_count': len(valid_df),
                    'calculation_date': pd.Timestamp.now().strftime('%Y-%m-%d')
                }
                
                logger.info(f"📊 Aggregates: {aggregates}")
                
                # Save to database
                conn = get_db_connection()
                cur = conn.cursor()
                
                # Insert each valid row
                for _, row in valid_df.iterrows():
                    try:
                        # Handle nullable fields
                        precip_type = row.get('Precip Type')
                        if pd.isna(precip_type):
                            precip_type = None
                        
                        wind_bearing = row.get('Wind Bearing (degrees)')
                        if pd.isna(wind_bearing):
                            wind_bearing = None
                        
                        loud_cover = row.get('Loud Cover')
                        if pd.isna(loud_cover):
                            loud_cover = 0
                        
                        cur.execute("""
                            INSERT INTO raw_sensor_data 
                            (formatted_date, summary, precip_type, temperature_c, 
                             apparent_temperature_c, humidity, wind_speed_kmh, 
                             wind_bearing, visibility_km, loud_cover, pressure_millibars, 
                             daily_summary, source_file, file_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            row.get('Formatted Date'),
                            row.get('Summary'),
                            precip_type,
                            float(row.get('Temperature (C)', 0)),
                            float(row.get('Apparent Temperature (C)', 0)),
                            float(row.get('Humidity', 0)),
                            float(row.get('Wind Speed (km/h)', 0)),
                            wind_bearing,
                            float(row.get('Visibility (km)', 0)),
                            int(loud_cover),
                            float(row.get('Pressure (millibars)', 0)),
                            row.get('Daily Summary'),
                            key,
                            key.split('/')[-1]
                        ))
                        rows_inserted += 1
                    except Exception as row_err:
                        logger.error(f"Error inserting row: {str(row_err)}")
                
                # Insert aggregates
                cur.execute("""
                    INSERT INTO sensor_aggregates 
                    (source_file, sensor_type, min_temperature, max_temperature, 
                     avg_temperature, stddev_temperature, min_humidity, max_humidity, 
                     avg_humidity, record_count, calculation_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    aggregates['source_file'],
                    aggregates['sensor_type'],
                    aggregates['min_temperature'],
                    aggregates['max_temperature'],
                    aggregates['avg_temperature'],
                    aggregates['stddev_temperature'],
                    aggregates['min_humidity'],
                    aggregates['max_humidity'],
                    aggregates['avg_humidity'],
                    aggregates['record_count'],
                    aggregates['calculation_date']
                ))
                
                conn.commit()
                cur.close()
                conn.close()
                
                logger.info(f"💾 Saved {rows_inserted} rows to database")
                
                # Send success message to SQS
                try:
                    sqs.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps({
                            'status': 'success',
                            'file': key,
                            'rows_processed': rows_inserted,
                            'rows_invalid': len(invalid_rows)
                        })
                    )
                except Exception as sqs_err:
                    logger.error(f"SQS send failed: {str(sqs_err)}")
            else:
                logger.warning("⚠️ No valid rows to insert")
            
            # STEP 7: Verification query
            try:
                verify_conn = get_db_connection()
                verify_cur = verify_conn.cursor()
                
                # Count rows inserted for this file
                verify_cur.execute("SELECT COUNT(*) FROM raw_sensor_data WHERE source_file = %s", (key,))
                inserted_count = verify_cur.fetchone()[0]
                logger.info(f"✅ Verification: {inserted_count} rows inserted for {key}")
                
                # Check aggregates
                verify_cur.execute("""
                    SELECT record_count, calculation_date 
                    FROM sensor_aggregates 
                    WHERE source_file = %s
                """, (key,))
                agg_result = verify_cur.fetchone()
                if agg_result:
                    logger.info(f"✅ Aggregates recorded: {agg_result[0]} rows on {agg_result[1]}")
                
                verify_cur.close()
                verify_conn.close()
                
            except Exception as verify_err:
                logger.error(f"Verification query failed: {str(verify_err)}")
            
            # STEP 8: ONLY AFTER successful processing, move the file to archive
            try:
                archive_key = f"processed/{key.split('/')[-1]}"
                
                # Copy to processed folder
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': key},
                    Key=archive_key
                )
                
                # Verify the copy worked
                s3.head_object(Bucket=bucket, Key=archive_key)
                
                # Delete original file
                s3.delete_object(Bucket=bucket, Key=key)
                
                logger.info(f"📦 Successfully archived file to: s3://{bucket}/{archive_key}")
                
            except Exception as e:
                logger.error(f"⚠️ File processed but archiving failed: {str(e)}")
                logger.info("Original file remains in incoming folder for manual review")
                # Don't raise exception - data is already in database
            
        except s3.exceptions.NoSuchKey:
            logger.warning(f"⚠️ File {key} no longer exists in S3. It may have been already processed.")
            continue
            
        except Exception as e:
            logger.error(f"💥 CRITICAL ERROR processing {key}: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Send error to SQS
            try:
                sqs.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps({
                        'status': 'error',
                        'file': key,
                        'error': str(e)
                    })
                )
            except:
                pass
            
            # Don't return error - continue processing other files
            continue
    
    logger.info("✅ Lambda function completed successfully")
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete')
    }