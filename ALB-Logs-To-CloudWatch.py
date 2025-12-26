import boto3
import json
import gzip
import io
import time

s3_client = boto3.client('s3')
logs_client = boto3.client('logs')

# 设置你想在 CloudWatch 看到的日志组名称
LOG_GROUP_NAME = '/aws/alb/access-logs'
LOG_STREAM_NAME = 'alb-stream'

def lambda_handler(event, context):
    # 1. 确保日志组存在
    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP_NAME)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    # 2. 确保日志流存在
    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=LOG_STREAM_NAME)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    # 3. 从 S3 事件中获取桶名和文件名
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    print(f"Processing file: {key} from bucket: {bucket}")

    # 4. 下载并解压 S3 中的日志文件 (.gz)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read()
        
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            log_data = gz.read().decode('utf-8')

        # 5. 准备发送到 CloudWatch 的数据
        log_events = []
        timestamp = int(round(time.time() * 1000))
        
        for line in log_data.splitlines():
            if line.strip():
                log_events.append({
                    'timestamp': timestamp,
                    'message': line
                })

        # 6. 批量写入 CloudWatch (如果数据量大，实际生产中需要分批处理)
        if log_events:
            # 获取 sequence token (如果是首次写入则不需要)
            # 注意：新版 CloudWatch API 通常不再强制要求 SequenceToken，但为了稳妥使用简单的 put_log_events
            logs_client.put_log_events(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=LOG_STREAM_NAME,
                logEvents=log_events
            )
            print(f"Successfully sent {len(log_events)} log lines to CloudWatch.")
            
    except Exception as e:
        print(f"Error processing object {key} from bucket {bucket}. Event: {json.dumps(event)}")
        raise e

    return {
        'statusCode': 200,
        'body': json.dumps('Log processing complete')
    }