from celery import Celery
import redis
import os

# === Redis setup ===
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
rdb = redis.Redis.from_url(REDIS_URL)

# === Celery setup ===
BROKER_URL = "amqp://cyvhztoo:BzEGwP10ORBC-w-uTLeyDzN74eohgd58@ostrich.lmq.cloudamqp.com/cyvhztoo"
celery_app = Celery("ens_tasks", broker=BROKER_URL, backend="rpc://")

# === Constants ===
SESSION_SET_KEY = "queued_session_ids"
NAME_VALIDATION_SET_KEY = "queued_name_validation_ids"

# === Screening Queue Task ===
@celery_app.task(name="process_session", queue="screening_queue")
def process_session(session_id):
    print(f"[Celery] 🔄 Processing session ID: {session_id}")
    rdb.srem(SESSION_SET_KEY, session_id)
    return f"[Celery] ✅ Finished processing session: {session_id}"

# === Name Validation Queue Task ===
@celery_app.task(name="validate_name", queue="name_validation_queue")
def validate_name(name_id):
    print(f"[Celery] 🔍 Validating name ID: {name_id}")
    rdb.srem(NAME_VALIDATION_SET_KEY, name_id)
    return f"[Celery] ✅ Name ID validated: {name_id}"

# === Session Queue Submitter ===
def submit_session(session_id):
    if rdb.sismember(SESSION_SET_KEY, session_id):
        return f"❌ Session ID '{session_id}' is already in the queue."
    rdb.sadd(SESSION_SET_KEY, session_id)
    process_session.delay(session_id)
    return f"✅ Session ID '{session_id}' added to queue."

# === Name Validation Queue Submitter ===
def submit_name_validation(name_id):
    if rdb.sismember(NAME_VALIDATION_SET_KEY, name_id):
        return f"❌ Name ID '{name_id}' is already in the queue."
    rdb.sadd(NAME_VALIDATION_SET_KEY, name_id)
    validate_name.delay(name_id)
    return f"✅ Name ID '{name_id}' added to queue."
