from flask import Flask, request, jsonify
from datetime import datetime
import threading
import time
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json


app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:pass@db:5432/logdb')

event_queue = []
queue_lock = threading.Lock()

def get_db_connection():
    """Get database connection with retry"""
    max_retries = 5
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"DB connection failed (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed to connect to database after {max_retries} attempts: {e}")


def init_database():
    """Initialize database schema"""
    print("Initializing database...")
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create processed_events table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(255) UNIQUE NOT NULL,
            topic VARCHAR(255) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            source VARCHAR(255) NOT NULL,
            payload JSONB,
            received_at TIMESTAMPTZ DEFAULT NOW(),
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    
    # Create indexes for performance
    cur.execute("CREATE INDEX IF NOT EXISTS idx_topic ON processed_events(topic)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON processed_events(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_received_at ON processed_events(received_at)")
    
    # Create stats table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY DEFAULT 1,
            received INTEGER DEFAULT 0,
            unique_processed INTEGER DEFAULT 0,
            duplicate_dropped INTEGER DEFAULT 0,
            CONSTRAINT single_row CHECK (id = 1)
        )
    """)
    
    # Initialize stats row
    cur.execute("""
        INSERT INTO stats (id, received, unique_processed, duplicate_dropped)
        VALUES (1, 0, 0, 0)
        ON CONFLICT (id) DO NOTHING
    """)
    
    conn.commit()
    cur.close()
    conn.close()
    
    print("✓ Database initialized")


def insert_event(topic, event_id, source, timestamp, payload):
    """Insert event into database with idempotency"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO processed_events (event_id, topic, timestamp, source, payload)
            VALUES (%s, %s, %s, %s, %s)
        """, (event_id, topic, timestamp, source, json.dumps(payload)))
        
        cur.execute("UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1")
        conn.commit()
        cur.close()
        conn.close()
        return (True, False)
        
    except psycopg2.IntegrityError:
        conn.rollback()
        cur.execute("UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1")
        conn.commit()
        cur.close()
        conn.close()
        return (True, True)
        
    except Exception as e:
        conn.rollback()
        cur.close()
        conn.close()
        print(f"Error inserting event: {e}")
        return (False, False)


def increment_received():
    """Increment received counter"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE stats SET received = received + 1 WHERE id = 1")
    conn.commit()
    cur.close()
    conn.close()


def get_events_from_db(topic=None, limit=100, offset=0):
    """Get list of processed events"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if topic:
        cur.execute("""
            SELECT event_id, topic, timestamp, source, payload, received_at
            FROM processed_events WHERE topic = %s
            ORDER BY received_at DESC LIMIT %s OFFSET %s
        """, (topic, limit, offset))
    else:
        cur.execute("""
            SELECT event_id, topic, timestamp, source, payload, received_at
            FROM processed_events
            ORDER BY received_at DESC LIMIT %s OFFSET %s
        """, (limit, offset))
    
    events = cur.fetchall()
    result = []
    for event in events:
        result.append({
            'event_id': event['event_id'],
            'topic': event['topic'],
            'timestamp': event['timestamp'].isoformat(),
            'source': event['source'],
            'payload': event['payload'],
            'received_at': event['received_at'].isoformat()
        })
    
    cur.close()
    conn.close()
    return result


def get_stats_from_db():
    """Get system statistics"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    cur.execute("SELECT * FROM stats WHERE id = 1")
    stats_row = cur.fetchone()
    
    cur.execute("""
        SELECT topic, COUNT(*) as count FROM processed_events
        GROUP BY topic ORDER BY count DESC
    """)
    topic_stats = cur.fetchall()
    
    cur.close()
    conn.close()
    
    topics = {}
    for row in topic_stats:
        topics[row['topic']] = row['count']
    
    return {
        'received': stats_row['received'],
        'unique_processed': stats_row['unique_processed'],
        'duplicate_dropped': stats_row['duplicate_dropped'],
        'topics': topics
    }


def worker():
    """Background worker"""
    print(f"Worker started: {threading.current_thread().name}")
    while True:
        event = None
        with queue_lock:
            if event_queue:
                event = event_queue.pop(0)
        
        if event:
            success, is_dup = insert_event(
                event['topic'], event['event_id'], event['source'],
                event['timestamp'], event['payload']
            )
            if is_dup:
                print(f"[DUP] {event['topic']}/{event['event_id']}")
            else:
                print(f"[OK]  {event['topic']}/{event['event_id']}")
        else:
            time.sleep(0.1)


@app.route('/health')
def health():
    return jsonify({'status': 'ok'})


@app.route('/publish', methods=['POST'])
def publish():
    data = request.json
    required = ['topic', 'event_id', 'timestamp', 'source', 'payload']
    for field in required:
        if field not in data:
            return jsonify({'error': f'Missing field: {field}'}), 400
    
    try:
        ts = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
    except:
        return jsonify({'error': 'Invalid timestamp format'}), 400
    
    with queue_lock:
        event_queue.append({
            'topic': data['topic'],
            'event_id': data['event_id'],
            'source': data['source'],
            'timestamp': ts,
            'payload': data['payload']
        })
    
    increment_received()
    return jsonify({'status': 'accepted', 'event_id': data['event_id']}), 202


@app.route('/publish/batch', methods=['POST'])
def publish_batch():
    data = request.json
    if 'events' not in data:
        return jsonify({'error': 'Missing events field'}), 400
    
    count = 0
    for event in data['events']:
        required = ['topic', 'event_id', 'timestamp', 'source', 'payload']
        for field in required:
            if field not in event:
                return jsonify({'error': f'Missing field: {field}'}), 400
        
        try:
            ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        except:
            return jsonify({'error': f"Invalid timestamp for event {event['event_id']}"}), 400
        
        with queue_lock:
            event_queue.append({
                'topic': event['topic'],
                'event_id': event['event_id'],
                'source': event['source'],
                'timestamp': ts,
                'payload': event['payload']
            })
        
        increment_received()
        count += 1
    
    return jsonify({'status': 'accepted', 'count': count}), 202


@app.route('/events')
def get_events():
    topic = request.args.get('topic')
    limit = int(request.args.get('limit', 100))
    offset = int(request.args.get('offset', 0))
    events = get_events_from_db(topic, limit, offset)
    return jsonify(events)


@app.route('/stats')
def get_stats():
    stats = get_stats_from_db()
    return jsonify(stats)


@app.route('/queue/status')
def queue_status():
    with queue_lock:
        queue_size = len(event_queue)
    return jsonify({'queue_size': queue_size, 'workers': 4})


if __name__ == '__main__':
    print("="*60)
    print("Initializing Aggregator Service...")
    print("="*60)
    
    init_database()
    
    workers = []
    for i in range(4):
        t = threading.Thread(target=worker, daemon=True, name=f'Worker-{i}')
        t.start()
        workers.append(t)
    
    print(f"✓ Started {len(workers)} worker threads")
    print("✓ Aggregator ready on http://0.0.0.0:8080")
    print("="*60)
    
    app.run(host='0.0.0.0', port=8080, threaded=True)
