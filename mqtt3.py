import paho.mqtt.client as mqtt
import requests
import json
from urllib.parse import quote, unquote

# 配置飞书 Webhook URL
FEISHU_WEBHOOK_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/"
# 配置 MinIO 的访问域名
MINIO_BASE_URL = "https://"  # 替换为您的 MinIO 服务器的域名

# MQTT 回调函数，当接收到消息时调用
def on_message(client, userdata, msg):
    print(f"Received message on topic {msg.topic}: {msg.payload.decode()}")

    # 解析 MinIO 发送的 MQTT 消息
    try:
        data = json.loads(msg.payload)
        event_name = data.get("EventName", "unknown event")
        bucket_name = data["Records"][0]["s3"]["bucket"]["name"]
        object_key = data["Records"][0]["s3"]["object"]["key"]

        # 解码对象键，确保显示时没有编码字符
        object_key_decoded = unquote(object_key)

        # 构造完整的文件 URL，保留路径中的斜杠
        object_key_encoded = quote(object_key_decoded, safe='/')
        object_url = f"{MINIO_BASE_URL}/{bucket_name}/{object_key_encoded}"

        # 构造飞书机器人消息，显示解码后的对象名称
        if "ObjectCreated" in event_name:
            # 推送包含访问链接的消息
            feishu_message = {
                "msg_type": "text",
                "content": {
                    "text": f"MinIO 事件通知：{event_name}，对象：{object_key_decoded}\n访问链接：{object_url}"
                }
            }
        elif "ObjectRemoved" in event_name:
            # 推送删除事件的消息，不包含访问链接
            feishu_message = {
                "msg_type": "text",
                "content": {
                    "text": f"MinIO 事件通知：{event_name}，对象：{object_key_decoded} 已被删除。"
                }
            }
        else:
            # 处理其他类型的事件
            feishu_message = {
                "msg_type": "text",
                "content": {
                    "text": f"MinIO 事件通知：{event_name}，对象：{object_key_decoded}"
                }
            }

        # 推送到飞书机器人
        response = requests.post(FEISHU_WEBHOOK_URL, json=feishu_message)
        if response.status_code == 200:
            print("Message sent to Feishu successfully.")
        else:
            print(f"Failed to send message to Feishu. Status code: {response.status_code}")
    except Exception as e:
        print(f"Error processing message: {e}")

# 配置 MQTT 客户端，指定协议版本以避免过时的 API 警告
client = mqtt.Client(protocol=mqtt.MQTTv311)  # 或 mqtt.MQTTv5

# 设置回调函数
client.on_message = on_message

# 连接到 MQTT Broker
broker_address = "localhost"
broker_port = 1883
client.connect(broker_address, broker_port, 60)

# 订阅主题
topic = "minio-events"
client.subscribe(topic)

# 保持订阅
client.loop_forever()
