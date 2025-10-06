import os
import re
import psycopg2
import pytz
import random # Import the random module
from datetime import datetime, timedelta
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables from .env file
load_dotenv()

# Initializes your app with your bot token
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# --- Globals & Cache ---
BOT_USER_ID = app.client.auth_test()["user_id"]
user_cache = {}
# A dictionary to store the bonus users for each channel
# Format: {'channel_id_1': {'user_id_a', 'user_id_b'}, 'channel_id_2': {'user_id_c', 'user_id_d'}}
daily_bonus_users = {}

def get_user_name(user_id):
    """
    Fetches a user's name from their ID, prioritizing real_name over display_name.
    Uses a cache to reduce API calls.
    """
    if user_id in user_cache:
        return user_cache[user_id]
    try:
        result = app.client.users_info(user=user_id)
        # Prioritize real_name, then display_name, then username.
        user_name = result['user']['profile'].get('real_name', result['user']['profile'].get('display_name', result['user']['name']))
        user_cache[user_id] = user_name
        return user_name
    except Exception as e:
        print(f"Error fetching user info for {user_id}: {e}")
        # Return a non-pingable fallback name instead of a mention.
        return f"User ({user_id})"

# --- Season Calculation ---
SEASON_START_DATE = datetime(2025, 10, 9, 0, 0, 0, tzinfo=pytz.timezone('America/Los_Angeles'))

def get_current_season_id():
    """
    Calculates the start date of the current season. Seasons are two weeks long.
    """
    now = datetime.now(pytz.timezone('America/Los_Angeles'))
    delta_days = (now - SEASON_START_DATE).days
    seasons_passed = delta_days // 14
    current_season_start = SEASON_START_DATE + timedelta(days=(seasons_passed * 14))
    return current_season_start.strftime('%Y-%m-%d')

# --- Scheduled Job Functions ---

def daily_bonus_job():
    """
    Selects two random users per active channel to be bonus targets for the day.
    """
    print("--- Running Daily Bonus Job ---")
    global daily_bonus_users # Declare that we are modifying the global variable
    
    # Clear the previous day's bonus users
    daily_bonus_users.clear()

    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Get a list of all channels that have ever had a spot
        cur.execute("SELECT DISTINCT channel_id FROM spots")
        active_channels = [row[0] for row in cur.fetchall()]

        for channel_id in active_channels:
            # For each channel, find all unique users who have ever participated
            cur.execute("""
                SELECT spotter_id FROM spots WHERE channel_id = %s
                UNION
                SELECT spotted_id FROM spots WHERE channel_id = %s
            """, (channel_id, channel_id))
            
            participants = [row[0] for row in cur.fetchall()]
            
            if len(participants) >= 2:
                # Select two unique users at random
                bonus_targets = random.sample(participants, 2)
                daily_bonus_users[channel_id] = set(bonus_targets)
                
                # Announce the bonus users in the channel
                user1_name = get_user_name(bonus_targets[0])
                user2_name = get_user_name(bonus_targets[1])
                announcement = f"🎉 *Daily Bonus!* 🎉\nToday's bonus targets are *{user1_name}* and *{user2_name}*! Spots of them are worth 2 points!"
                app.client.chat_postMessage(channel=channel_id, text=announcement)
                print(f"--- Bonus users for channel {channel_id}: {bonus_targets} ---")

        cur.close()
        conn.close()
        print("--- Daily Bonus Job Finished ---")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in daily_bonus_job: {error}")

def end_of_season_job():
    """
    This function runs at the end of a season. It finds the winner,
    announces them in all relevant channels, and prepares for the new season.
    """
    # This function is now correctly implemented based on previous iteration
    print("--- Running End of Season Job ---")
    
    new_season_start_str = get_current_season_id()
    new_season_start_dt = datetime.strptime(new_season_start_str, '%Y-%m-%d').astimezone(pytz.timezone('America/Los_Angeles'))
    previous_season_start_dt = new_season_start_dt - timedelta(days=14)
    previous_season_id = previous_season_start_dt.strftime('%Y-%m-%d')
    print(f"--- Processing results for previous season: {previous_season_id} ---")

    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT channel_id FROM spots WHERE season_id = %s", (previous_season_id,))
        channels = cur.fetchall()

        for channel_tuple in channels:
            channel_id = channel_tuple[0]
            
            winner_query = """
                SELECT spotter_id, SUM(points) AS total_score
                FROM spots
                WHERE season_id = %s AND channel_id = %s AND is_valid = TRUE
                GROUP BY spotter_id
                ORDER BY total_score DESC
                LIMIT 1;
            """
            cur.execute(winner_query, (previous_season_id, channel_id))
            winner_result = cur.fetchone()

            announcement = f"🏆 A new Spotting Season has begun! 📸\n\n"
            if winner_result:
                winner_id, winner_score = winner_result
                winner_name = get_user_name(winner_id)
                announcement += f"Congratulations to *{winner_name}* for winning the last season with {int(winner_score)} spots!"
            else:
                announcement += "No spots were recorded in the last season. A fresh start!"
            
            app.client.chat_postMessage(channel=channel_id, text=announcement)
        
        cur.close()
        conn.close()
        print("--- End of Season Job Finished ---")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in end_of_season_job: {error}")

# --- Database Setup ---
def setup_database():
    """
    Connects to the database and creates the 'spots' table.
    """
    create_table_command = """
    CREATE TABLE IF NOT EXISTS spots (
        id SERIAL PRIMARY KEY,
        spotter_id TEXT NOT NULL,
        spotted_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        message_ts TEXT NOT NULL,
        image_url TEXT NOT NULL,
        points INTEGER NOT NULL DEFAULT 1,
        season_id TEXT NOT NULL,
        is_valid BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (message_ts, spotted_id)
    );
    """
    conn = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            print("🔴 DATABASE_URL is not set. Please check your .env file.")
            return
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(create_table_command)
        conn.commit()
        cur.close()
        print("✅ Database table 'spots' is ready.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error while connecting to PostgreSQL: {error}")
    finally:
        if conn is not None:
            conn.close()

# --- Bot Event Listeners ---
def is_spot_message_and_not_command(message):
    text = message.get("text", "")
    has_keyword = re.search(r"\b(spot|spotted)\b", text, re.IGNORECASE)
    is_command = text.strip().startswith(f"<@{BOT_USER_ID}>")
    return has_keyword and not is_command

@app.message(matchers=[is_spot_message_and_not_command])
def handle_spot_message(message, say):
    """
    Core logic for handling a spot. Now awards bonus points.
    """
    print("\n--- DEBUG: `handle_spot_message` was triggered. ---")

    if 'user' not in message or 'files' not in message or 'text' not in message:
        return

    spotter_id = message['user']
    text = message['text']
    channel_id = message['channel'] # Get channel ID
    
    mentioned_users = set(re.findall(r"<@(\w+)>", text))
    if not mentioned_users:
        return

    successful_spots = 0
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        for spotted_id in mentioned_users:
            if spotter_id == spotted_id:
                continue
            
            points_to_award = 1
            if channel_id in daily_bonus_users and spotted_id in daily_bonus_users[channel_id]:
                points_to_award = 2
                print(f"--- DEBUG: Awarding 2 bonus points for spotting {spotted_id} in {channel_id}. ---")

            insert_command = """
            INSERT INTO spots (spotter_id, spotted_id, channel_id, message_ts, image_url, season_id, points)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            
            spot_data = (
                spotter_id,
                spotted_id,
                channel_id,
                message['ts'],
                message['files'][0]['url_private'],
                get_current_season_id(),
                points_to_award
            )
            
            cur.execute(insert_command, spot_data)
            successful_spots += 1
        
        conn.commit()
        cur.close()
        conn.close()

        if successful_spots > 0:
            app.client.reactions_add(
                channel=message['channel'],
                timestamp=message['ts'],
                name="white_check_mark"
            )

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 DEBUG: An error occurred during database operation: {error}")

# **FIXED**: Using the subtype listener as a workaround for the UI bug.
@app.event({"type": "message", "subtype": "message_deleted"})
def handle_message_deletion(event):
    """
    Handles the deletion of a message by removing the corresponding spot from the database.
    """
    print("\n--- DEBUG: `handle_message_deletion` (subtype) was triggered. ---")
    
    if 'previous_message' not in event or 'ts' not in event['previous_message']:
        print("--- DEBUG: No previous_message or ts found in deletion event. Skipping. ---")
        return

    deleted_ts = event['previous_message']['ts']
    print(f"--- DEBUG: A message with timestamp {deleted_ts} was deleted. Checking database. ---")

    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        delete_command = "DELETE FROM spots WHERE message_ts = %s"
        
        cur.execute(delete_command, (deleted_ts,))
        
        if cur.rowcount > 0:
            print(f"--- SUCCESS: Deleted {cur.rowcount} spot record(s) with timestamp {deleted_ts}. ---")
        else:
            print(f"--- INFO: Deleted message {deleted_ts} was not a spot record. No action taken. ---")

        conn.commit()
        cur.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 DEBUG: An error occurred during message deletion handling: {error}")

# --- Command Handlers and Listeners ---
def handle_spotboard_command(message, say):
    """
    Generates and posts the seasonal spotboard for the current channel.
    """
    try:
        channel_id = message['channel']
        current_season = get_current_season_id()
        
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = """
            SELECT spotter_id, SUM(points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND season_id = %s AND channel_id = %s
            GROUP BY spotter_id
            ORDER BY total_score DESC
            LIMIT 5;
        """
        cur.execute(query, (current_season, channel_id))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No spots have been recorded in this channel this season yet!")
            return

        leaderboard_text = f"*Spotboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row
            score = int(score)
            user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score} spots\n"
        
        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling spotboard command: {error}")
        say("Sorry, I had trouble fetching the spotboard.")

def handle_caughtboard_command(message, say):
    """
    Generates and posts the seasonal caughtboard for the current channel.
    """
    try:
        channel_id = message['channel']
        current_season = get_current_season_id()
        
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = """
            SELECT spotted_id, SUM(points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND season_id = %s AND channel_id = %s
            GROUP BY spotted_id
            ORDER BY total_score DESC
            LIMIT 5;
        """
        cur.execute(query, (current_season, channel_id))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No one has been spotted in this channel this season yet!")
            return

        leaderboard_text = f"*Caughtboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row
            score = int(score)
            user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - caught {score} times\n"
        
        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling caughtboard command: {error}")
        say("Sorry, I had trouble fetching the caughtboard.")


def handle_alltime_spotboard_command(message, say):
    """
    Generates and posts the all-time spotboard for the current channel.
    """
    try:
        channel_id = message['channel']
        
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = """
            SELECT spotter_id, SUM(points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND channel_id = %s
            GROUP BY spotter_id
            ORDER BY total_score DESC
            LIMIT 5;
        """
        cur.execute(query, (channel_id,))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No spots have ever been recorded in this channel!")
            return

        leaderboard_text = f"*All-time Spotboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row
            score = int(score)
            user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score} spots\n"
        
        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling all-time spotboard command: {error}")
        say("Sorry, I had trouble fetching the all-time spotboard.")

def handle_alltime_caughtboard_command(message, say):
    """
    Generates and posts the all-time caughtboard for the current channel.
    """
    try:
        channel_id = message['channel']
        
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Query for all-time caught scores
        query = """
            SELECT spotted_id, SUM(points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND channel_id = %s
            GROUP BY spotted_id
            ORDER BY total_score DESC
            LIMIT 5;
        """
        cur.execute(query, (channel_id,))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No one has ever been caught in this channel!")
            return

        leaderboard_text = f"*All-time Caughtboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row
            score = int(score)
            user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - caught {score} times\n"
        
        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling all-time caughtboard command: {error}")
        say("Sorry, I had trouble fetching the all-time caughtboard.")

# --- Keyword Command Listeners ---

@app.message(re.compile(r"^spotboard$", re.IGNORECASE))
def handle_spotboard_keyword(message, say):
    handle_spotboard_command(message, say)

@app.message(re.compile(r"^caughtboard$", re.IGNORECASE))
def handle_caughtboard_keyword(message, say):
    handle_caughtboard_command(message, say)

@app.message(re.compile(r"^(alltimespotboard|all time spot board)$", re.IGNORECASE))
def handle_alltime_spotboard_keyword(message, say):
    handle_alltime_spotboard_command(message, say)

@app.message(re.compile(r"^(alltimecaughtboard|all time caught board)$", re.IGNORECASE))
def handle_alltime_caughtboard_keyword(message, say):
    handle_alltime_caughtboard_command(message, say)

@app.event("app_mention")
def handle_mention(event, say):
    """
    Handles leaderboard commands when the bot is @-mentioned.
    """
    command_text = event['text'].strip().lower()

    if "alltimecaughtboard" in command_text or "all time caught board" in command_text:
        handle_alltime_caughtboard_command(event, say)
    elif "alltimespotboard" in command_text or "all time spot board" in command_text:
        handle_alltime_spotboard_command(event, say)
    elif "caughtboard" in command_text:
        handle_caughtboard_command(event, say)
    elif "spotboard" in command_text:
        handle_spotboard_command(event, say)
    else:
        say(f"Hi there, <@{event['user']}>! Try one of our leaderboard commands: `spotboard`, `caughtboard`, `alltimespotboard`, or `alltimecaughtboard`.")


# --- Main Application Execution ---
if __name__ == "__main__":
    setup_database()
    
    scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Los_Angeles'))
    
    # Schedule the end of season job
    scheduler.add_job(
        end_of_season_job, 
        'cron', 
        day_of_week='thu', 
        hour=0, 
        minute=0, 
        week='*/2', 
        start_date='2025-10-09 00:00:00'
    )
    
    # **NEW**: Schedule the daily bonus job
    scheduler.add_job(
        daily_bonus_job,
        'cron',
        hour=0,
        minute=0
    )

    scheduler.start()
    print("⏰ Scheduler started. All jobs are scheduled.")
    
    print("⚡️ Spot Bot is running!")
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()

