from datetime import datetime, timedelta
import random
import json

def generate_marketing_logs(num_records=5000):
    """
    Generate semi-structured log files from marketing campaigns
    Issues:
    - Inconsistent log formats
    - Mixed structured and unstructured data
    - Different timestamp formats
    - Parsing challenges
    """
    
    log_levels = ['INFO', 'DEBUG', 'WARN', 'ERROR', 'FATAL', 'info', 'debug']
    campaigns = [f"CAMP{i:04d}" for i in range(1, 51)]
    channels = ['email', 'sms', 'push_notification', 'social_media', 'display_ad']
    actions = ['sent', 'delivered', 'opened', 'clicked', 'converted', 'bounced', 'unsubscribed']
    
    start_time = datetime.now() - timedelta(days=30)
    
    logs = []
    
    for i in range(num_records):
        timestamp = start_time + timedelta(seconds=random.randint(0, 30*24*60*60))
        
        # Choose log format (inconsistent across entries)
        log_format = random.choice(['json', 'structured', 'unstructured', 'mixed'])
        
        if log_format == 'json':
            # JSON structured log
            log_entry = {
                'timestamp': timestamp.isoformat(),
                'level': random.choice(log_levels),
                'campaign_id': random.choice(campaigns),
                'customer_id': f"CUST{random.randint(1, 2000):06d}",
                'channel': random.choice(channels),
                'action': random.choice(actions),
                'message_id': f"MSG{random.randint(100000, 999999)}",
                'metadata': {
                    'device': random.choice(['mobile', 'desktop', 'tablet']),
                    'os': random.choice(['iOS', 'Android', 'Windows', 'macOS']),
                    'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
                }
            }
            log_line = json.dumps(log_entry)
            
        elif log_format == 'structured':
            # Apache/NGINX style log
            log_line = (
                f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} "
                f"[{random.choice(log_levels)}] "
                f"campaign={random.choice(campaigns)} "
                f"customer={f'CUST{random.randint(1, 2000):06d}'} "
                f"channel={random.choice(channels)} "
                f"action={random.choice(actions)} "
                f"status={'success' if random.random() > 0.1 else 'failure'}"
            )
            
        elif log_format == 'unstructured':
            # Free-form log message
            messages = [
                f"Email campaign {random.choice(campaigns)} sent to customer CUST{random.randint(1, 2000):06d}",
                f"Customer clicked link in campaign {random.choice(campaigns)}",
                f"SMS delivered successfully via {random.choice(channels)}",
                f"Push notification bounced for user CUST{random.randint(1, 2000):06d}",
                f"Customer unsubscribed from campaign {random.choice(campaigns)}",
                f"ERROR: Failed to send email - SMTP timeout",
                f"Campaign {random.choice(campaigns)} conversion recorded",
            ]
            log_line = f"{timestamp.strftime('%d/%b/%Y:%H:%M:%S')} {random.choice(messages)}"
            
        else:  # mixed
            # Combination of formats
            if random.random() > 0.5:
                log_line = (
                    f"{timestamp.isoformat()} | "
                    f"LEVEL={random.choice(log_levels)} | "
                    f"CAMPAIGN={random.choice(campaigns)} | "
                    f"ACTION={random.choice(actions)} | "
                    f"CUSTOMER=CUST{random.randint(1, 2000):06d}"
                )
            else:
                log_line = (
                    f"[{timestamp.strftime('%Y%m%d %H:%M:%S.%f')[:-3]}] "
                    f"{random.choice(log_levels)}: "
                    f"Campaign {random.choice(campaigns)} - "
                    f"{random.choice(actions)} event for customer "
                    f"CUST{random.randint(1, 2000):06d}"
                )
        
        logs.append(log_line)
    
    return logs


def generate_email_campaign_csv():
    """
    Generate email campaign performance data in CSV
    """
    import pandas as pd
    
    campaigns_data = []
    
    for i in range(1, 51):
        campaign_id = f"CAMP{i:04d}"
        
        sent = random.randint(1000, 50000)
        delivered = int(sent * random.uniform(0.85, 0.98))
        opened = int(delivered * random.uniform(0.15, 0.35))
        clicked = int(opened * random.uniform(0.10, 0.30))
        converted = int(clicked * random.uniform(0.01, 0.15))
        
        campaigns_data.append({
            'campaign_id': campaign_id,
            'campaign_name': f"Campaign_{i}_{random.choice(['promo', 'newsletter', 'announcement'])}",
            'channel': random.choice(['email', 'sms', 'push']),
            'start_date': (datetime.now() - timedelta(days=random.randint(1, 90))).strftime('%Y-%m-%d'),
            'end_date': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'status': random.choice(['active', 'Active', 'completed', 'Completed', 'paused', '']),
            'target_audience': random.choice(['all_customers', 'premium', 'new', 'inactive']),
            'sent_count': sent,
            'delivered_count': delivered,
            'opened_count': opened,
            'clicked_count': clicked,
            'converted_count': converted,
            'bounce_rate': round((sent - delivered) / sent * 100, 2) if sent > 0 else 0,
            'open_rate': round(opened / delivered * 100, 2) if delivered > 0 else 0,
            'click_rate': round(clicked / opened * 100, 2) if opened > 0 else 0,
            'conversion_rate': round(converted / clicked * 100, 2) if clicked > 0 else 0,
            'revenue': round(converted * random.uniform(10, 500), 2),
            'cost': round(sent * random.uniform(0.01, 0.05), 2),
            'roi': ''  # Will calculate
        })
    
    df = pd.DataFrame(campaigns_data)
    df['roi'] = ((df['revenue'] - df['cost']) / df['cost'] * 100).round(2)
    
    # Introduce some calculation errors
    mask = np.random.random(len(df)) < 0.05
    df.loc[mask, 'roi'] = None
    
    return df


# Generate log file
print("Generating marketing campaign logs...")
logs = generate_marketing_logs(5000)

with open('marketing_campaigns.log', 'w') as f:
    for log in logs:
        f.write(log + '\n')

print(f"✓ Generated marketing_campaigns.log with {len(logs)} log entries")

# Generate campaign performance CSV
print("\nGenerating campaign performance data...")
import numpy as np
campaign_df = generate_email_campaign_csv()
campaign_df.to_csv('campaign_performance.csv', index=False)
print(f"✓ Generated campaign_performance.csv with {len(campaign_df)} campaigns")

print("\n" + "="*60)
print("LOG FILE CHARACTERISTICS:")
print("="*60)
print("✓ Mixed log formats (JSON, structured, unstructured)")
print("✓ Inconsistent timestamp formats")
print("✓ Semi-structured data requiring regex parsing")
print("✓ Different log levels and severity")
print("✓ Embedded JSON within logs")
print("✓ Variable field presence")

print("\n" + "="*60)
print("SAMPLE LOG ENTRIES:")
print("="*60)
for i in range(10):
    print(logs[i])

print("\n" + "="*60)
print("CAMPAIGN PERFORMANCE DATA:")
print("="*60)
print(campaign_df.head())
print("\nCampaign Statistics:")
print(f"Total campaigns: {len(campaign_df)}")
print(f"Total emails sent: {campaign_df['sent_count'].sum():,}")
print(f"Average open rate: {campaign_df['open_rate'].mean():.2f}%")
print(f"Average conversion rate: {campaign_df['conversion_rate'].mean():.2f}%")