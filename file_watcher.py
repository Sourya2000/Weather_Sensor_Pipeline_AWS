import time
import os
import boto3
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# AWS Configuration
BUCKET_NAME = 'sensor-pipeline-bucket-unique'  # Your bucket name
PROCESSED_DIR = './data_processed'
QUARANTINE_DIR = './data_quarantine'
WATCH_DIR = './data'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAYS = [2, 5, 10]  # seconds between retries
FILE_STABILITY_CHECKS = 3    # number of size checks to confirm file is stable
STABILITY_CHECK_INTERVAL = 1 # seconds between checks

# Create AWS client
s3_client = boto3.client('s3')

def is_file_stable(file_path):
    """
    Check if file is completely written and stable
    Returns: (is_stable, file_size)
    """
    if not os.path.exists(file_path):
        logger.debug(f"File does not exist: {file_path}")
        return False, 0
    
    sizes = []
    try:
        for i in range(FILE_STABILITY_CHECKS):
            size = os.path.getsize(file_path)
            sizes.append(size)
            logger.info(f"   Check {i+1}: {size:,} bytes")
            
            if i < FILE_STABILITY_CHECKS - 1:
                time.sleep(STABILITY_CHECK_INTERVAL)
        
        # File is stable if all size checks are the same and > 0
        is_stable = len(set(sizes)) == 1 and sizes[0] > 0
        return is_stable, sizes[0] if sizes else 0
        
    except (OSError, IOError) as e:
        logger.error(f"Error checking file: {str(e)}")
        return False, 0

def upload_with_retry(file_path, s3_key, file_size):
    """
    Upload file to S3 with retry mechanism
    Returns: bool (success/failure)
    """
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"📤 Upload attempt {attempt + 1}/{MAX_RETRIES} for {s3_key}")
            
            # Upload the file
            s3_client.upload_file(file_path, BUCKET_NAME, s3_key)
            
            # Verify upload by checking file size in S3
            response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            s3_size = response['ContentLength']
            
            if s3_size == file_size:
                logger.info(f"✅ Upload verified: {s3_size:,} bytes")
                return True
            else:
                raise Exception(f"Size mismatch: local={file_size:,}, S3={s3_size:,}")
                
        except Exception as e:
            logger.warning(f"⚠️ Attempt {attempt + 1} failed: {str(e)}")
            
            if attempt < MAX_RETRIES - 1:
                delay = RETRY_DELAYS[attempt]
                logger.info(f"⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"❌ All {MAX_RETRIES} attempts failed for {s3_key}")
                return False
    
    return False

def move_file(file_path, destination_dir, reason=""):
    """
    Move file to destination directory with timestamp if needed
    Returns: new_path or None if failed
    """
    try:
        os.makedirs(destination_dir, exist_ok=True)
        file_name = os.path.basename(file_path)
        
        # Add timestamp if there's a reason (for quarantine)
        if reason:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_file_name = f"{reason}_{timestamp}_{file_name}"
        else:
            new_file_name = file_name
        
        new_path = os.path.join(destination_dir, new_file_name)
        
        # If file already exists in destination, add number
        counter = 1
        while os.path.exists(new_path):
            name, ext = os.path.splitext(new_file_name)
            new_path = os.path.join(destination_dir, f"{name}_{counter}{ext}")
            counter += 1
        
        os.rename(file_path, new_path)
        logger.info(f"📁 Moved to {destination_dir}: {os.path.basename(new_path)}")
        return new_path
        
    except Exception as e:
        logger.error(f"❌ Failed to move file {file_path}: {str(e)}")
        return None

class NewFileHandler(FileSystemEventHandler):
    """Handles new file events in the watched directory"""
    
    def on_created(self, event):
        # Only process CSV files, ignore directories
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)
        
        logger.info(f"✅ New file detected: {file_name}")
        logger.info(f"⏳ Waiting for file to be completely written...")
        
        # Wait for file to be stable
        max_wait = 30  # Maximum 30 seconds to wait
        start_time = time.time()
        file_stable = False
        file_size = 0
        
        while time.time() - start_time < max_wait:
            is_stable, size = is_file_stable(file_path)
            
            if is_stable:
                file_stable = True
                file_size = size
                logger.info(f"📄 File is stable: {file_size:,} bytes")
                break
                
            logger.info("⏳ File still changing or empty, waiting...")
            time.sleep(2)
        
        if not file_stable:
            logger.error(f"❌ File not stable after {max_wait} seconds")
            move_file(file_path, QUARANTINE_DIR, "unstable")
            return
        
        # Upload to S3 with retry
        s3_key = f"incoming/{file_name}"
        upload_success = upload_with_retry(file_path, s3_key, file_size)
        
        if upload_success:
            # Move to processed folder
            move_file(file_path, PROCESSED_DIR)
            logger.info(f"🎉 Successfully processed: {file_name}")
        else:
            # Move to quarantine with failure reason
            move_file(file_path, QUARANTINE_DIR, "upload_failed")
            logger.error(f"💔 Failed to upload after {MAX_RETRIES} attempts: {file_name}")

def check_aws_credentials():
    """Verify AWS credentials are working"""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        logger.info(f"✅ AWS credentials valid - can access bucket: {BUCKET_NAME}")
        return True
    except Exception as e:
        logger.error(f"❌ AWS credentials issue: {str(e)}")
        logger.error("Please run 'aws configure' to set up your credentials")
        return False

if __name__ == "__main__":
    # Display banner
    logger.info("=" * 60)
    logger.info("🔍 SENSOR PIPELINE FILE WATCHER")
    logger.info("=" * 60)
    
    # Create required directories
    os.makedirs(WATCH_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)
    
    # Check AWS credentials
    if not check_aws_credentials():
        logger.error("Exiting due to AWS credential issues")
        exit(1)
    
    # Display configuration
    logger.info(f"📂 Watching folder: {os.path.abspath(WATCH_DIR)}")
    logger.info(f"📤 Uploading to: s3://{BUCKET_NAME}/incoming/")
    logger.info(f"✅ Processed files go to: {os.path.abspath(PROCESSED_DIR)}")
    logger.info(f"⚠️ Quarantine files go to: {os.path.abspath(QUARANTINE_DIR)}")
    logger.info(f"🔄 Max retries: {MAX_RETRIES}")
    logger.info(f"⏱️  Stability checks: {FILE_STABILITY_CHECKS} x {STABILITY_CHECK_INTERVAL}s")
    logger.info("=" * 60)
    
    # Set up the file watcher
    event_handler = NewFileHandler()
    observer = Observer()
    observer.schedule(event_handler, WATCH_DIR, recursive=False)
    observer.start()
    
    logger.info(f"✅ File watcher is running. Press Ctrl+C to stop.")
    
    try:
        # Keep running until interrupted
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("👋 Shutting down file watcher...")
        observer.stop()
    
    observer.join()
    logger.info("✅ File watcher stopped cleanly")
    
