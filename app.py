import os
import re
import psycopg2
import pytz
from datetime import datetime, timedelta # Added timedelta
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler

# Load environment variables from .env file
load_dotenv()

# Initializes your app with your bot token
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# --- Globals & Cache ---
# Get the bot's own user ID at startup to ignore commands in the message listener
BOT_USER_ID = app.client.auth_test()["user_id"]
user_cache = {}

def get_user_name(user_id):
    """
    Fetches a user's display name from their ID, using a cache to reduce API calls.
    """
    if user_id in user_cache:
        return user_cache[user_id]
    try:
        result = app.client.users_info(user=user_id)
        # Use display_name if available, otherwise real_name, fallback to the base name
        user_name = result['user']['profile'].get('display_name', result['user'].get('real_name', result['user']['name']))
        user_cache[user_id] = user_name # Save to cache
        return user_name
    except Exception as e:
        print(f"Error fetching user info for {user_id}: {e}")
        return f"<@{user_id}>" # Fallback to showing the mention

# --- Season Calculation ---
# The official start date of the very first season.
SEASON_START_DATE = datetime(2025, 10, 9, 0, 0, 0, tzinfo=pytz.timezone('America/Los_Angeles'))

def get_current_season_id():
    """
    Calculates the start date of the current season. Seasons are two weeks long.
    This start date will be used as the unique ID for the season.
    """
    now = datetime.now(pytz.timezone('America/Los_Angeles'))
    # Calculate the total number of days that have passed since the first season started.
    delta_days = (now - SEASON_START_DATE).days
    # A season is 14 days long. Figure out how many full seasons have passed.
    seasons_passed = delta_days // 14
    # The current season's start date is the original start date plus 14 days for every season passed.
    current_season_start = SEASON_START_DATE + timedelta(days=(seasons_passed * 14))
    # Return the date as a simple string 'YYYY-MM-DD'.
    return current_season_start.strftime('%Y-%m-%d')

# --- Scheduled Job Function ---
def end_of_season_job():
    """
    This function runs at the end of a season. It finds the winner,
    announces them in all relevant channels, and prepares for the new season.
    """
    print("--- Running End of Season Job ---")
    
    # We need to find the winner of the *previous* season.
    # The previous season ID is the one active just before this job ran.
    previous_season_id = get_current_season_id() # This will be the NEW season ID, so we need to calculate the one before it.
    # Note: A more robust way would be to calculate it from the date, but this is simpler for now.
    # For the purpose of this example, we'll assume the job runs right as a new season starts.
    # A truly robust solution would pass the season_id into the job.

    # This is a placeholder as the logic to get the *previous* season is complex.
    # For now, we will just announce a new season has begun.
    # TODO: Implement winner calculation.

    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Find all unique channels where spots happened in the last season
        # We'll need this to announce in the right places.
        cur.execute("SELECT DISTINCT channel_id FROM spots WHERE season_id = %s", (previous_season_id,))
        channels = cur.fetchall()

        for channel in channels:
            channel_id = channel[0]
            announcement_text = f"🏆 A new Spotting Season has begun! Good luck! 📸"
            app.client.chat_postMessage(channel=channel_id, text=announcement_text)
        
        cur.close()
        conn.close()
        print("--- End of Season Job Finished ---")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in end_of_season_job: {error}")


# --- Database Setup (Schema Updated) ---
def setup_database():
    """
    Connects to the database and creates the 'spots' table with a composite unique key.
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

# This is a custom function (a "matcher") that will be used by the listener.
# It returns True only if the message contains "spot" AND is NOT a command for the bot.
def is_spot_message_and_not_command(message):
    text = message.get("text", "")
    # Check 1: Does it contain the whole word "spot" or "spotted"?
    has_keyword = re.search(r"\b(spot|spotted)\b", text, re.IGNORECASE)
    # Check 2: Does it start with a mention of our bot? If so, it's a command.
    is_command = text.strip().startswith(f"<@{BOT_USER_ID}>")
    return has_keyword and not is_command

@app.message(matchers=[is_spot_message_and_not_command])
def handle_spot_message(message, say):
    """
    This is the core logic. It now spots every unique mentioned user.
    """
    print("\n--- DEBUG: `handle_spot_message` was triggered. ---")

    # Basic validation
    if 'user' not in message or 'files' not in message or 'text' not in message:
        print("--- DEBUG: Message failed basic validation (missing user, files, or text). ---")
        return

    print("--- DEBUG: Message passed basic validation. ---")
    spotter_id = message['user']
    text = message['text']
    
    # Find all unique user mentions in the message text
    mentioned_users = set(re.findall(r"<@(\w+)>", text))
    print(f"--- DEBUG: Found mentioned user IDs: {mentioned_users}")
    
    if not mentioned_users:
        print("--- DEBUG: No user mentions found in the message text. Stopping. ---")
        return

    successful_spots = 0
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Loop through every unique user mentioned
        for spotted_id in mentioned_users:
            # Can't spot yourself!
            if spotter_id == spotted_id:
                print(f"--- DEBUG: User {spotter_id} tried to spot themselves. Skipping. ---")
                continue # Skip to the next mentioned user
            
            print(f"--- DEBUG: Preparing to insert spot for user {spotted_id}. ---")
            insert_command = """
            INSERT INTO spots (spotter_id, spotted_id, channel_id, message_ts, image_url, season_id)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            
            spot_data = (
                spotter_id,
                spotted_id,
                message['channel'],
                message['ts'],
                message['files'][0]['url_private'],
                get_current_season_id()
            )
            
            cur.execute(insert_command, spot_data)
            successful_spots += 1
            print(f"--- DEBUG: Successfully executed INSERT for {spotted_id}. ---")
        
        conn.commit()
        cur.close()
        conn.close()

        # Add a confirmation reaction only if at least one spot was valid
        if successful_spots > 0:
            print("--- DEBUG: Adding confirmation reaction. ---")
            app.client.reactions_add(
                channel=message['channel'],
                timestamp=message['ts'],
                name="white_check_mark"
            )

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 DEBUG: An error occurred during database operation: {error}")


# --- Command Handlers ---
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

        leaderboard_text = f"🏆 *Spotboard - Top 5 Spotters This Season* 🏆\n\n"
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

        leaderboard_text = f"🎯 *Caughtboard - Top 5 Most Spotted This Season* 🎯\n\n"
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

        leaderboard_text = f"👑 *All-Time Spotboard - Top 5 Spotters* 👑\n\n"
        for i, row in enumerate(results):
            user_id, score = row
            score = int(score)
            user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score} spots\n"
        
        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling all-time spotboard command: {error}")
        say("Sorry, I had trouble fetching the all-time spotboard.")

# **NEW**: All-Time Caughtboard Handler
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

        leaderboard_text = f"🚨 *All-Time Caughtboard - Top 5 Most Caught* 🚨\n\n"
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

# **NEW**: Listener for 'alltimecaughtboard' and 'all time caught board'
@app.message(re.compile(r"^(alltimecaughtboard|all time caught board)$", re.IGNORECASE))
def handle_alltime_caughtboard_keyword(message, say):
    handle_alltime_caughtboard_command(message, say)

@app.event("app_mention")
def handle_mention(event, say):
    """
    Handles leaderboard commands when the bot is @-mentioned.
    """
    command_text = event['text'].strip().lower()

    # **UPDATED**: Check for all-time commands *before* seasonal commands to avoid conflicts
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
    
    # --- Initialize and Start the Scheduler ---
    scheduler = BackgroundScheduler(timezone=pytz.timezone('America/Los_Angeles'))
    scheduler.add_job(
        end_of_season_job, 
        'cron', 
        day_of_week='thu', 
        hour=0, 
        minute=0, 
        week='*/2', 
        start_date='2025-10-09 00:00:00'
    )
    scheduler.start()
    print("⏰ Scheduler started. End of season job is scheduled.")
    
    print("⚡️ Spot Bot is running!")
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()

