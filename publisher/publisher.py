import requests
import time
import random
from datetime import datetime
import os

API_URL = os.getenv('API_URL', 'http://localhost:8080')
TOTAL_EVENTS = int(os.getenv('TOTAL_EVENTS', 20000))
DUPLICATE_RATE = float(os.getenv('DUPLICATE_RATE', 0.3))

TOPICS = ['user.login', 'user.logout', 'order.created', 'order.completed', 'payment.processed']
SOURCES = ['web-app', 'mobile-app', 'api-gateway']

print("="*70)
print("EVENT PUBLISHER")
print("="*70)
print(f"Target URL      : {API_URL}")
print(f"Total Events    : {TOTAL_EVENTS}")
print(f"Duplicate Rate  : {DUPLICATE_RATE*100}%")
print("="*70)

print("\nWaiting for aggregator...")
for i in range(30):
    try:
        r = requests.get(f"{API_URL}/health", timeout=2)
        if r.status_code == 200:
            print("✓ Aggregator is ready!\n")
            break
    except:
        pass
    time.sleep(1)
else:
    print("✗ Aggregator not ready after 30s")
    exit(1)

event_pool = []
sent_count = 0
duplicate_count = 0
failed_count = 0

start_time = time.time()

print("Publishing events...")
print("-"*70)

for i in range(TOTAL_EVENTS):
    if event_pool and random.random() < DUPLICATE_RATE:
        event = random.choice(event_pool)
        duplicate_count += 1
    else:
        # Generate new event
        event = {
            'topic': random.choice(TOPICS),
            'event_id': f'event-{i:06d}',
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'source': random.choice(SOURCES),
            'payload': {
                'user_id': random.randint(1000, 9999),
                'amount': random.randint(10, 1000),
                'ip': f'192.168.{random.randint(0,255)}.{random.randint(1,254)}'
            }
        }
        event_pool.append(event)
        
        if len(event_pool) > 1000:
            event_pool.pop(0)
    
    try:
        r = requests.post(
            f"{API_URL}/publish",
            json=event,
            timeout=5
        )
        if r.status_code == 202:
            sent_count += 1
        else:
            failed_count += 1
    except Exception as e:
        failed_count += 1
        if failed_count < 5:  
            print(f"Error: {e}")
    

    if (i + 1) % 1000 == 0:
        elapsed = time.time() - start_time
        rate = sent_count / elapsed if elapsed > 0 else 0
        print(f"[{i+1:5d}/{TOTAL_EVENTS}] Sent: {sent_count:5d} | Dup: {duplicate_count:5d} | Failed: {failed_count:3d} | Rate: {rate:6.0f}/s")
    
    if i % 100 == 0:
        time.sleep(0.05)

# Final summary
elapsed = time.time() - start_time

print("-"*70)
print("\n" + "="*70)
print("PUBLISHING COMPLETED")
print("="*70)
print(f"Total Sent      : {sent_count}")
print(f"Duplicates Sent : {duplicate_count} ({duplicate_count/sent_count*100:.1f}%)")
print(f"Failed          : {failed_count}")
print(f"Duration        : {elapsed:.1f}s")
print(f"Average Rate    : {sent_count/elapsed:.0f} events/s")
print("="*70)

try:
    r = requests.get(f"{API_URL}/stats", timeout=5)
    if r.status_code == 200:
        stats = r.json()
        print("\nAGGREGATOR STATS:")
        print(f"  Received        : {stats['received']}")
        print(f"  Unique Processed: {stats['unique_processed']}")
        print(f"  Duplicates      : {stats['duplicate_dropped']}")
        print(f"  Topics          : {stats['topics']}")
        print("="*70)
except:
    pass
