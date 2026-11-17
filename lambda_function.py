import json
import os
import uuid
import re
import urllib.request
from datetime import datetime
import boto3

dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
s3 = boto3.client('s3')

TABLE_NAME = 'BrandPulseReviews'
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
LOG_BUCKET_NAME = os.environ.get('LOG_BUCKET_NAME', '')

POSITIVE_WORDS = {
    'amazing', 'good', 'great', 'excellent', 'love', 'fast', 'helpful', 'best',
    'awesome', 'perfect', 'happy', 'satisfied', 'recommend', 'nice', 'super',
    'ดี', 'เยี่ยม', 'ชอบ', 'ประทับใจ', 'รวดเร็ว', 'สุดยอด', 'น่ารัก', 'คุ้มค่า', 'อร่อย', 'ขอบคุณ'
}
NEGATIVE_WORDS = {
    'terrible', 'bad', 'worst', 'broken', 'slow', 'hate', 'awful', 'poor',
    'horrible', 'useless', 'disappointed', 'scam', 'fail', 'error', 'angry', 'refund',
    'แย่', 'ห่วย', 'พัง', 'ช้า', 'โกง', 'ผิดหวัง', 'โมโห', 'ไม่ดี', 'เสียหาย', 'คืนเงิน'
}

def analyze_sentiment_nlp(text):
    clean_text = text.lower()
    words = set(re.findall(r'\w+', clean_text))
    pos_hits = sum(1 for w in POSITIVE_WORDS if w in clean_text or w in words)
    neg_hits = sum(1 for w in NEGATIVE_WORDS if w in clean_text or w in words)
    
    if pos_hits + neg_hits == 0:
        return 'NEUTRAL', 78.50
    elif pos_hits > neg_hits:
        return 'POSITIVE', round(min(99.20, 75.0 + (pos_hits * 8.5)), 2)
    elif neg_hits > pos_hits:
        return 'NEGATIVE', round(min(99.50, 78.0 + (neg_hits * 8.5)), 2)
    return 'MIXED', 65.00

def fetch_external_social_feed():
    # ดึงโพสต์ล่าสุด 3 รายการจาก Hacker News Official Public API (เสถียรและไม่บล็อก IP ของ AWS)
    top_url = "https://hacker-news.firebaseio.com/v0/newstories.json"
    with urllib.request.urlopen(top_url, timeout=10) as res:
        story_ids = json.loads(res.read().decode('utf-8'))[:3]
        
    titles = []
    for sid in story_ids:
        item_url = f"https://hacker-news.firebaseio.com/v0/item/{sid}.json"
        with urllib.request.urlopen(item_url, timeout=10) as item_res:
            story = json.loads(item_res.read().decode('utf-8'))
            if story and story.get('title'):
                titles.append(story['title'])
    return titles

def process_and_save_review(review_text, source="Web Portal"):
    sentiment, confidence = analyze_sentiment_nlp(review_text)
    review_id = str(uuid.uuid4())[:8]
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    item = {
        'review_id': review_id,
        'text': f"[{source}] {review_text}",
        'sentiment': sentiment,
        'confidence': str(confidence),
        'timestamp': timestamp
    }
    dynamodb.Table(TABLE_NAME).put_item(Item=item)

    if LOG_BUCKET_NAME:
        s3.put_object(
            Bucket=LOG_BUCKET_NAME,
            Key=f"logs/{sentiment.lower()}/{review_id}.json",
            Body=json.dumps(item, ensure_ascii=False),
            ContentType='application/json'
        )

    alert_sent = False
    if sentiment == 'NEGATIVE' and SNS_TOPIC_ARN:
        alert_msg = (
            f"🚨 AUTOMATED BRAND CRISIS ALERT 🚨\n\n"
            f"Source: {source}\n"
            f"Review ID: {review_id}\n"
            f"Sentiment: {sentiment} ({confidence}% confidence)\n"
            f"Message: \"{review_text}\"\n"
            f"Time (UTC): {timestamp}\n"
            f"Audit log saved to S3: {LOG_BUCKET_NAME}"
        )
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f'[URGENT] Negative Mention Detected ({source})',
            Message=alert_msg
        )
        alert_sent = True

    return item, alert_sent

def lambda_handler(event, context):
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
    }
    
    # โหมดอัตโนมัติจาก Amazon EventBridge (Level 4)
    if event.get('source') == 'aws.events' or 'httpMethod' not in event:
        try:
            titles = fetch_external_social_feed()
            results = []
            for title in titles:
                item, _ = process_and_save_review(title, source="Auto-HackerNews API")
                results.append(item)
            return {'statusCode': 200, 'body': json.dumps({'mode': 'AUTOMATED_SCAN', 'processed': len(results), 'items': results})}
        except Exception as e:
            return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}

    # โหมดเรียกใช้งานผ่านหน้าเว็บ API Gateway
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': ''}

    try:
        if event.get('httpMethod') == 'GET':
            response = dynamodb.Table(TABLE_NAME).scan(Limit=20)
            items = sorted(response.get('Items', []), key=lambda x: x['timestamp'], reverse=True)
            return {'statusCode': 200, 'headers': headers, 'body': json.dumps(items)}

        body = json.loads(event.get('body', '{}'))
        review_text = body.get('text', '').strip()
        if not review_text:
            return {'statusCode': 400, 'headers': headers, 'body': json.dumps({'error': 'Text is required'})}

        item, alert_sent = process_and_save_review(review_text, source="Web Portal")
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({'message': 'Analyzed successfully', 'data': item, 'alert_sent': alert_sent})
        }
    except Exception as e:
        return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': str(e)})}