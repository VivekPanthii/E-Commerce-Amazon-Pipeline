import json
import random
from datetime import datetime, timedelta
import uuid

# Generate clickstream/event data in JSON format
def generate_clickstream_events(num_events=10000):
    """
    Simulates web/app event logs with realistic messiness:
    - Nested JSON structures
    - Inconsistent schema
    - Missing fields
    - Different event types
    """
    
    events = []
    start_date = datetime(2024, 10, 1)
    
    event_types = ['page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
                   'checkout_start', 'checkout_complete', 'search', 'filter_apply',
                   'review_submit', 'wishlist_add']
    
    devices = ['desktop', 'mobile', 'tablet', 'Mobile', 'Desktop', 'MOBILE']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'chrome', 'safari','Opera','opera','Brave','brave', None]
    operating_systems = ['Windows', 'macOS', 'iOS', 'Android', 'Linux', 'windows', None]
    
    for i in range(num_events):
        event_time = start_date + timedelta(seconds=random.randint(0, 3600*24*45))
        
        # Base event structure
        event = {
            'event_id': str(uuid.uuid4()),
            'event_type': random.choice(event_types),
            'timestamp': event_time.isoformat() if random.random() > 0.02 else event_time.strftime('%Y-%m-%d %H:%M:%S'),
            'session_id': f"sess_{random.randint(1, 5000):08d}",
            'user_id': f"CUST{random.randint(1, 2000):06d}" if random.random() > 0.3 else None,
        }
        
        # Device info - sometimes nested, sometimes flat
        if random.random() > 0.1:
            event['device'] = {
                'type': random.choice(devices),
                'browser': random.choice(browsers),
                'os': random.choice(operating_systems),
                'screen_resolution': random.choice(['1920x1080', '1366x768', '375x667', '414x896', None])
            }
        else:
            # Flat structure (schema inconsistency)
            event['device_type'] = random.choice(devices)
            event['browser'] = random.choice(browsers)
        
        # Location data - sometimes missing
        if random.random() > 0.15:
            event['location'] = {
                'country': random.choice(['US', 'USA', 'United States', 'CA', 'UK', None]),
                'state': random.choice(['CA', 'NY', 'TX', 'FL', None]),
                'city': random.choice(['Los Angeles', 'New York', 'Chicago', None]),
                'ip_address': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
            }
        
        # Event-specific data
        if event['event_type'] == 'product_view':
            event['product'] = {
                'product_id': f"PROD{random.randint(1, 1000):06d}",
                'product_name': f"Product Name {random.randint(1, 100)}",
                'category': random.choice(['Electronics', 'Clothing', 'Books', None]),
                'price': round(random.uniform(10, 500), 2),
                'in_stock': random.choice([True, False, 'true', 'false', 1, 0])
            }
            
        elif event['event_type'] == 'add_to_cart':
            event['cart_action'] = {
                'product_id': f"PROD{random.randint(1, 1000):06d}",
                'quantity': random.randint(1, 5),
                'price': round(random.uniform(10, 500), 2)
            }
            
        elif event['event_type'] == 'search':
            event['search_query'] = random.choice([
                'laptop', 'phone case', 'running shoes', 'headphones',
                '', None  # Empty searches
            ])
            event['results_count'] = random.randint(0, 500) if event['search_query'] else 0
            
        elif event['event_type'] == 'checkout_complete':
            event['order'] = {
                'order_id': f"ORD{random.randint(1000, 9999):08d}",
                'total_amount': round(random.uniform(20, 2000), 2),
                'items_count': random.randint(1, 10)
            }
        
        # Page info - sometimes present
        if random.random() > 0.2:
            event['page'] = {
                'url': f"/page/{random.randint(1, 100)}",
                'referrer': random.choice(['google.com', 'facebook.com', 'direct', None]),
                'load_time_ms': random.randint(100, 5000)
            }
        
        # Marketing attribution - sometimes missing
        if random.random() > 0.6:
            event['marketing'] = {
                'campaign_id': f"CAMP{random.randint(1, 50):04d}" if random.random() > 0.2 else None,
                'source': random.choice(['google', 'facebook', 'email', 'organic', None]),
                'medium': random.choice(['cpc', 'social', 'email', None])
            }
        
        # Extra metadata field - inconsistent
        if random.random() > 0.7:
            event['metadata'] = {
                'user_agent': 'Mozilla/5.0 (compatible; bot)',
                'ab_test_variant': random.choice(['A', 'B', 'control', None])
            }
        
        events.append(event)
    
    return events


# Generate and save events
print("Generating clickstream events...")
events = generate_clickstream_events(10000)

# Save as JSON Lines format (common for streaming data)
with open('clickstream_events.jsonl', 'w') as f:
    for event in events:
        f.write(json.dumps(event) + '\n')

print(f"✓ Generated clickstream_events.jsonl with {len(events)} events")

# Also save first 100 as pretty-printed JSON for inspection
with open('clickstream_sample.json', 'w') as f:
    json.dump(events[:100], f, indent=2)

print("✓ Generated clickstream_sample.json (first 100 events, pretty-printed)")

print("\n" + "="*60)
print("CLICKSTREAM DATA CHARACTERISTICS:")
print("="*60)
print("✓ Nested JSON structures (varying depth)")
print("✓ Inconsistent schema across events")
print("✓ Missing fields in many records")
print("✓ Different data types for same logical field")
print("✓ NULL/empty values in critical fields")
print("✓ Timestamp format inconsistencies")
print("✓ Mixed event types requiring different parsing")
print("\n" + "="*60)
print("EVENT TYPES DISTRIBUTION:")
print("="*60)

event_type_counts = {}
for event in events:
    event_type = event.get('event_type', 'unknown')
    event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1

for event_type, count in sorted(event_type_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{event_type}: {count} events")

print("\n" + "="*60)
print("SAMPLE EVENTS:")
print("="*60)
for i in range(3):
    print(f"\nEvent {i+1}:")
    print(json.dumps(events[i], indent=2))