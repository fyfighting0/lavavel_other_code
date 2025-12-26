import boto3
import os
import json


#  AppConfig ID
APP_ID = "dkrr9yc"
ENV_ID = "npgc4a2"
PROF_ID = "pfmv6jk"

# 你的 ECS 信息
ECS_CLUSTER = "laravel-cluster-0"
ECS_SERVICE = "laravel-service"
CONTAINER_NAME = "app"

# ★★★ 新增：SNS 主题 ARN (请填入你的 SNS ARN) ★★★
SNS_TOPIC_ARN = "arn:aws:sns:ap-northeast-1:785186658813:Failure-Alert"
# ==========================================

appconfig = boto3.client('appconfigdata')
ecs = boto3.client('ecs')
sns = boto3.client('sns') # 初始化 SNS 客户端

def send_sns_notification(subject, message):
    """发送 SNS 通知的辅助函数"""
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject,
            Message=message
        )
        print(f"SNS 通知已发送: {subject}")
    except Exception as e:
        print(f"发送 SNS 失败: {str(e)}")

def lambda_handler(event, context):
    print(f"开始同步配置: {ECS_SERVICE}...")
    
    # 使用 try...except 包裹所有逻辑，以便捕获错误
    try:
        # 1. 下载配置
        try:
            session = appconfig.start_configuration_session(
                ApplicationIdentifier=APP_ID,
                EnvironmentIdentifier=ENV_ID,
                ConfigurationProfileIdentifier=PROF_ID
            )
            token = session['InitialConfigurationToken']
            resp = appconfig.get_latest_configuration(ConfigurationToken=token)
            raw_content = resp['Configuration'].read().decode('utf-8')
        except Exception as e:
            raise Exception(f"AppConfig 读取失败: {str(e)}")

        # 2. 解析 .env 为 ECS 环境变量格式
        new_env = []
        for line in raw_content.splitlines():
            line = line.strip()
            if not line or line.startswith('#'): continue
            if '=' in line:
                key, value = line.split('=', 1)
                new_env.append({
                    'name': key.strip(),
                    'value': value.strip().strip("'").strip('"')
                })

        print(f"解析到 {len(new_env)} 个变量")

        # 3. 获取当前 ECS 任务定义
        srv_resp = ecs.describe_services(cluster=ECS_CLUSTER, services=[ECS_SERVICE])
        if not srv_resp['services']:
            raise Exception(f"找不到 ECS 服务: {ECS_SERVICE}")
            
        current_task_arn = srv_resp['services'][0]['taskDefinition']
        
        task_resp = ecs.describe_task_definition(taskDefinition=current_task_arn)
        task_def = task_resp['taskDefinition']

        # 4. 注入环境变量
        found = False
        for container in task_def['containerDefinitions']:
            if container['name'] == CONTAINER_NAME:
                container['environment'] = new_env
                found = True
                break
        
        if not found:
            raise Exception(f"找不到容器名为 {CONTAINER_NAME} 的容器，请检查配置")

        # 5. 清理字段 (注册新版本必须删除这些只读字段)
        for k in ['taskDefinitionArn', 'revision', 'status', 'requiresAttributes', 'compatibilities', 'registeredAt', 'registeredBy']:
            task_def.pop(k, None)
        
        # 6. 注册并更新服务
        new_task = ecs.register_task_definition(**task_def)
        new_arn = new_task['taskDefinition']['taskDefinitionArn']
        
        ecs.update_service(
            cluster=ECS_CLUSTER, 
            service=ECS_SERVICE, 
            taskDefinition=new_arn, 
            forceNewDeployment=True
        )
        
        # ==========================================
        #  成功逻辑：发送 SNS 成功通知
        # ==========================================
        success_msg = f"AppConfig部署成功。\nECS服务 '{ECS_SERVICE}' 已更新环境变量。\n新的任务定义: {new_arn}"
        send_sns_notification(
            subject="[INFO] ECS 配置更新成功", 
            message=success_msg
        )
        
        return f"成功更新 ECS 环境变量，新版本: {new_arn}"

    except Exception as e:
        # ==========================================
        #  失败逻辑：发送 SNS 报警通知
        # ==========================================
        error_msg = f"AppConfig部署同步脚本执行失败！\n\n错误详情:\n{str(e)}\n\n请立即检查 Lambda 日志。"
        print(f"脚本执行出错: {str(e)}")
        
        send_sns_notification(
            subject="[ALARM] ECS 配置更新失败", 
            message=error_msg
        )
        
        # 重新抛出异常，让 Lambda 执行状态标记为 Failed
        raise e