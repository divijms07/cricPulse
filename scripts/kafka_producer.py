import json
import time
from kafka import KafkaProducer

# Kafka config
KAFKA_TOPIC = 'match_events'
KAFKA_BROKER = 'localhost:9092'

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load Cricsheet JSON file
with open('../data/ipl25/csk_vs_gt.json', 'r') as f:
    match_data = json.load(f)

print("Flattening JSON into ball-by-ball events...")

# Flatten JSON into list of ball events
ball_events = []

for inning in match_data['innings']:
    team = inning['team']
    for over in inning['overs']:
        over_num = over['over']
        for delivery_num, delivery in enumerate(over['deliveries'], start=1):
            event = {
                "match_id": match_data['info']['event']['match_number'],
                "inning": team,
                "over": over_num,
                "ball": delivery_num,
                "batsman": delivery['batter'],
                "bowler": delivery['bowler'],
                "non_striker": delivery.get('non_striker'),
                "runs_batter": delivery['runs']['batter'],
                "runs_extras": delivery['runs']['extras'],
                "runs_total": delivery['runs']['total'],
                "extras_wides": delivery.get('extras', {}).get('wides', 0),
                "extras_no_balls": delivery.get('extras', {}).get('noballs', 0),
                "team": team
            }
            ball_events.append(event)

print(f"Total ball events to send: {len(ball_events)}")

# Stream events to Kafka
for ball in ball_events:
    producer.send(KAFKA_TOPIC, ball)
    print(f"Sent ball: {ball['over']}.{ball['ball']} | {ball['batsman']} vs {ball['bowler']} | Runs: {ball['runs_total']}")
    time.sleep(1)  # simulate live feed

producer.flush()
print("All events sent!")
