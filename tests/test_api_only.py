import pytest
import requests
import time
from concurrent.futures import ThreadPoolExecutor


BASE_URL = "http://localhost:8080"


def test_01_health_check():
    """Test 1: Health check"""
    r = requests.get(f"{BASE_URL}/health")
    assert r.status_code == 200
    print("✓ Test 1 passed")


def test_02_publish_event():
    """Test 2: Publish single event"""
    event = {
        'topic': 'test',
        'event_id': f'test-{int(time.time())}',
        'timestamp': '2025-12-18T12:00:00Z',
        'source': 'pytest',
        'payload': {'data': 'test'}
    }
    r = requests.post(f"{BASE_URL}/publish", json=event)
    assert r.status_code == 202
    print("✓ Test 2 passed")


def test_03_invalid_event():
    """Test 3: Invalid event validation"""
    event = {'topic': 'test'}  # Missing required fields
    r = requests.post(f"{BASE_URL}/publish", json=event)
    assert r.status_code in [400, 422]
    print("✓ Test 3 passed")


def test_04_invalid_timestamp():
    """Test 4: Invalid timestamp validation"""
    event = {
        'topic': 'test',
        'event_id': 'invalid-ts',
        'timestamp': 'not-a-timestamp',
        'source': 'pytest',
        'payload': {}
    }
    r = requests.post(f"{BASE_URL}/publish", json=event)
    assert r.status_code == 400
    print("✓ Test 4 passed")


def test_05_get_stats():
    """Test 5: Stats endpoint"""
    r = requests.get(f"{BASE_URL}/stats")
    assert r.status_code == 200
    data = r.json()
    assert 'received' in data
    assert 'unique_processed' in data
    assert 'duplicate_dropped' in data
    assert 'topics' in data
    print("✓ Test 5 passed")


def test_06_get_events():
    """Test 6: Get events endpoint"""
    r = requests.get(f"{BASE_URL}/events")
    assert r.status_code == 200
    assert isinstance(r.json(), list)
    print("✓ Test 6 passed")


def test_07_duplicate_via_stats():
    """Test 7: Idempotency via stats tracking"""
    base_id = int(time.time())
    event = {
        'topic': 'dup-api',
        'event_id': f'dup-{base_id}',
        'timestamp': '2025-12-18T12:00:00Z',
        'source': 'pytest',
        'payload': {}
    }
    
    # Get initial stats
    r1 = requests.get(f"{BASE_URL}/stats")
    stats1 = r1.json()
    
    # Send 3x (1 unique + 2 duplicates)
    for _ in range(3):
        requests.post(f"{BASE_URL}/publish", json=event)
    
    time.sleep(2)
    
    # Get final stats
    r2 = requests.get(f"{BASE_URL}/stats")
    stats2 = r2.json()
    
    # Should have: +3 received, +1 unique, +2 duplicates
    received_diff = stats2['received'] - stats1['received']
    dup_diff = stats2['duplicate_dropped'] - stats1['duplicate_dropped']
    
    assert received_diff >= 3, f"Expected >=3 received, got {received_diff}"
    assert dup_diff >= 2, f"Expected >=2 duplicates, got {dup_diff}"
    print("✓ Test 7 passed (idempotency via stats)")


def test_08_concurrent_submissions():
    """Test 8: Concurrent duplicate submissions"""
    base_id = int(time.time())
    event = {
        'topic': 'conc-api',
        'event_id': f'conc-{base_id}',
        'timestamp': '2025-12-18T12:00:00Z',
        'source': 'pytest',
        'payload': {}
    }
    
    def send():
        return requests.post(f"{BASE_URL}/publish", json=event)
    
    # Send 10x concurrent (same event)
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(send) for _ in range(10)]
        results = [f.result() for f in futures]
    
    # All should be accepted
    for r in results:
        assert r.status_code == 202
    
    print("✓ Test 8 passed (concurrent submissions accepted)")


def test_09_batch_publish():
    """Test 9: Batch publish"""
    base_id = int(time.time())
    batch = {
        'events': [
            {
                'topic': 'batch-api',
                'event_id': f'batch-{base_id}-{i}',
                'timestamp': '2025-12-18T12:00:00Z',
                'source': 'pytest',
                'payload': {'index': i}
            }
            for i in range(10)
        ]
    }
    
    r = requests.post(f"{BASE_URL}/publish/batch", json=batch)
    assert r.status_code == 202
    data = r.json()
    assert 'count' in data or 'status' in data
    print("✓ Test 9 passed (batch publish)")


def test_10_query_by_topic():
    """Test 10: Query events by topic"""
    base_id = int(time.time())
    topic = f'query-test-{base_id}'
    
    # Publish event
    event = {
        'topic': topic,
        'event_id': f'query-{base_id}',
        'timestamp': '2025-12-18T12:00:00Z',
        'source': 'pytest',
        'payload': {'test': True}
    }
    requests.post(f"{BASE_URL}/publish", json=event)
    time.sleep(2)
    
    # Query by topic
    r = requests.get(f"{BASE_URL}/events?topic={topic}")
    assert r.status_code == 200
    events = r.json()
    
    # Should have at least our event
    assert len(events) >= 1, "Event not found in query"
    print("✓ Test 10 passed (query by topic)")


def test_11_stats_consistency():
    """Test 11: Stats consistency (received >= processed)"""
    r = requests.get(f"{BASE_URL}/stats")
    assert r.status_code == 200
    stats = r.json()
    
    # Basic math check
    received = stats['received']
    processed = stats['unique_processed']
    duplicates = stats['duplicate_dropped']
    
    # received should be >= processed
    assert received >= processed, f"Received ({received}) < Processed ({processed})"
    print(f"✓ Test 11 passed (Stats: R={received}, P={processed}, D={duplicates})")


def test_12_throughput():
    """Test 12: Basic throughput"""
    base_id = int(time.time())
    start = time.time()
    
    for i in range(50):
        event = {
            'topic': 'perf-api',
            'event_id': f'perf-{base_id}-{i}',
            'timestamp': '2025-12-18T12:00:00Z',
            'source': 'pytest',
            'payload': {}
        }
        requests.post(f"{BASE_URL}/publish", json=event)
    
    duration = time.time() - start
    rate = 50 / duration
    
    print(f"\n  Throughput: {rate:.0f} events/s")
    assert rate > 20, f"Throughput too low: {rate:.0f} events/s"
    print("✓ Test 12 passed")


def test_13_batch_with_duplicates():
    """Test 13: Batch with internal duplicates"""
    base_id = int(time.time())
    
    # Batch contains duplicates within itself
    batch = {
        'events': [
            {'topic': 'batch-dup', 'event_id': f'bd-{base_id}-1', 'timestamp': '2025-12-18T12:00:00Z', 'source': 'test', 'payload': {}},
            {'topic': 'batch-dup', 'event_id': f'bd-{base_id}-1', 'timestamp': '2025-12-18T12:00:00Z', 'source': 'test', 'payload': {}},  # duplicate
            {'topic': 'batch-dup', 'event_id': f'bd-{base_id}-2', 'timestamp': '2025-12-18T12:00:00Z', 'source': 'test', 'payload': {}},
            {'topic': 'batch-dup', 'event_id': f'bd-{base_id}-2', 'timestamp': '2025-12-18T12:00:00Z', 'source': 'test', 'payload': {}},  # duplicate
        ]
    }
    
    r1 = requests.get(f"{BASE_URL}/stats")
    stats1 = r1.json()
    
    r = requests.post(f"{BASE_URL}/publish/batch", json=batch)
    assert r.status_code == 202
    
    time.sleep(2)
    
    r2 = requests.get(f"{BASE_URL}/stats")
    stats2 = r2.json()
    
    # Should process 2 unique, 2 duplicates
    dup_diff = stats2['duplicate_dropped'] - stats1['duplicate_dropped']
    assert dup_diff >= 2, f"Expected >=2 duplicates in batch, got {dup_diff}"
    print("✓ Test 13 passed (batch with duplicates)")


def test_14_multiple_topics():
    """Test 14: Multiple topics handling"""
    base_id = int(time.time())
    topics = [f'topic-{base_id}-A', f'topic-{base_id}-B', f'topic-{base_id}-C']
    
    # Publish to multiple topics
    for i, topic in enumerate(topics):
        event = {
            'topic': topic,
            'event_id': f'mt-{base_id}-{i}',
            'timestamp': '2025-12-18T12:00:00Z',
            'source': 'pytest',
            'payload': {'topic_index': i}
        }
        requests.post(f"{BASE_URL}/publish", json=event)
    
    time.sleep(2)
    
    # Verify each topic has events
    for topic in topics:
        r = requests.get(f"{BASE_URL}/events?topic={topic}")
        assert r.status_code == 200
        events = r.json()
        assert len(events) >= 1, f"No events found for {topic}"
    
    print("✓ Test 14 passed (multiple topics)")


def test_15_concurrent_different_events():
    """Test 15: Concurrent submissions of different events"""
    base_id = int(time.time())
    
    def send_unique(index):
        event = {
            'topic': 'conc-unique',
            'event_id': f'cu-{base_id}-{index}',
            'timestamp': '2025-12-18T12:00:00Z',
            'source': 'pytest',
            'payload': {'index': index}
        }
        return requests.post(f"{BASE_URL}/publish", json=event)
    
    # Send 20 different events concurrently
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(send_unique, i) for i in range(20)]
        results = [f.result() for f in futures]
    
    # All should be accepted
    for r in results:
        assert r.status_code == 202
    
    time.sleep(3)
    
    # Query and verify
    r = requests.get(f"{BASE_URL}/events?topic=conc-unique&limit=30")
    events = r.json()
    
    # Should have all 20 unique events
    unique_ids = set(e['event_id'] for e in events if 'conc-unique' in e.get('topic', ''))
    assert len(unique_ids) >= 20, f"Expected >=20 unique events, got {len(unique_ids)}"
    print(f"✓ Test 15 passed (concurrent different: {len(unique_ids)} unique)")


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
