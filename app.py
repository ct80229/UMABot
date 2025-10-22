import os
import re
import psycopg2
import pytz
import random # Import the random module
import requests
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from PIL import Image

# Load environment variables from .env file
load_dotenv()

# Initializes your app with your bot token
app = App(token=os.environ.get("SLACK_BOT_TOKEN"))

# --- Globals & Cache ---
BOT_USER_ID = app.client.auth_test()["user_id"]
ADMIN_USER_ID = "U06HB636NHG" # Your User ID
user_cache = {}
daily_bonus_users = {}
# The explosion images are now loaded from a local directory
EXPLOSIONS_DIR = "explosions"

# --- Season Calculation (FIXED SCHEDULE) ---
# The master schedule is now a fixed constant and will not be changed.
SEASON_START_DATE = datetime(2025, 10, 9, 0, 0, 0, tzinfo=pytz.timezone('America/Los_Angeles'))
# This dictionary will store the timestamp of the last manual reset for each channel.
# Format: {"channel_id": datetime_object}
manual_reset_timestamps = {}

def get_current_season_id():
    """
    Calculates the start date of the current season based on the FIXED anchor date.
    """
    now = datetime.now(pytz.timezone('America/Los_Angeles'))
    delta_days = (now - SEASON_START_DATE).days
    seasons_passed = delta_days // 14
    current_season_start = SEASON_START_DATE + timedelta(days=(seasons_passed * 14))
    return current_season_start.strftime('%Y-%m-%d')


# --- Reusable Season Logic ---
def announce_season_winner(season_id_to_process, channel_id, is_manual_reset=False):
    """
    A helper function to find the winner for a given season and post announcements.
    Can be used by both scheduled jobs and manual resets.
    """
    print(f"--- Announcing winner for season: {season_id_to_process} in channel {channel_id} ---")
    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        winner_query = """
            SELECT spotter_id, SUM(spotter_points) AS total_score
            FROM spots
            WHERE season_id = %s AND channel_id = %s AND is_valid = TRUE
            GROUP BY spotter_id
            ORDER BY total_score DESC
            LIMIT 1;
        """
        cur.execute(winner_query, (season_id_to_process, channel_id))
        winner_result = cur.fetchone()

        if is_manual_reset:
            announcement = "✅ *Manual Reset Complete!*\n\n"
        else:
            announcement = f"🏆 A new Spotting Season has begun! 📸\n\n"

        if winner_result:
            winner_id, winner_score = winner_result
            winner_name = get_user_name(winner_id)
            period = "interim season" if is_manual_reset else "last season"
            announcement += f"Congratulations to *{winner_name}* for winning the {period} with {int(winner_score)} spots!"
        else:
            announcement += "No spots were recorded in the last period. A fresh start!"

        app.client.chat_postMessage(channel=channel_id, text=announcement)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in announce_season_winner: {error}")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()


# --- Scheduled Job Functions ---
def daily_bonus_job():
    """
    Selects two random users per active channel to be bonus targets for the day.
    """
    print("--- Running Daily Bonus Job ---")
    global daily_bonus_users
    daily_bonus_users.clear() # Clear previous day's targets globally

    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT channel_id FROM spots")
        active_channels = [row[0] for row in cur.fetchall()]
        print(f"--- Found active channels for bonus job: {active_channels} ---")

        new_bonus_assignments = {} # Use a temporary dict to build the new assignments

        for channel_id in active_channels:
            cur.execute("""
                SELECT DISTINCT user_id FROM (
                    SELECT spotter_id as user_id FROM spots WHERE channel_id = %s
                    UNION
                    SELECT spotted_id as user_id FROM spots WHERE channel_id = %s
                ) as participants
            """, (channel_id, channel_id))

            participants = [row[0] for row in cur.fetchall()]
            print(f"--- Found participants for channel {channel_id}: {participants} ---")


            if len(participants) >= 2:
                bonus_targets = random.sample(participants, 2)
                # Store in the temporary dictionary
                new_bonus_assignments[channel_id] = set(bonus_targets)

                user1_name = get_user_name(bonus_targets[0])
                user2_name = get_user_name(bonus_targets[1])
                announcement = f"🎉 *Daily Bonus!* 🎉\nToday's bonus targets are *{user1_name}* and *{user2_name}*! Spots of them are worth 2 points!"
                try:
                    app.client.chat_postMessage(channel=channel_id, text=announcement)
                    print(f"--- Bonus users announced for channel {channel_id}: {bonus_targets} ---")
                except Exception as api_error:
                    print(f"🔴 Error posting bonus announcement to {channel_id}: {api_error}")
            else:
                 print(f"--- Not enough participants in channel {channel_id} to assign bonus targets. ---")

        # Atomically update the global variable
        daily_bonus_users = new_bonus_assignments
        print("--- Daily Bonus Job Finished ---")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in daily_bonus_job: {error}")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()


def end_of_season_job():
    """
    Scheduled job that runs automatically. It determines the previous season and announces the winner.
    """
    print("--- Running Scheduled End of Season Job ---")
    global manual_reset_timestamps

    current_season_start_str = get_current_season_id()
    current_season_start_dt = datetime.strptime(current_season_start_str, '%Y-%m-%d').astimezone(pytz.timezone('America/Los_Angeles'))
    previous_season_start_dt = current_season_start_dt - timedelta(days=14)
    previous_season_id = previous_season_start_dt.strftime('%Y-%m-%d')

    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT channel_id FROM spots WHERE season_id = %s", (previous_season_id,))
        channels = [row[0] for row in cur.fetchall()]
        cur.close() # Close cursor after fetching channels
        conn.close() # Close connection after fetching channels

        for channel_id in channels:
            # Reconnect or pass connection details if needed by announce_season_winner
            announce_season_winner(previous_season_id, channel_id, is_manual_reset=False)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error getting channels in end_of_season_job: {error}")
    finally:
        if cur is not None and not cur.closed:
             cur.close()
        if conn is not None and not conn.closed:
             conn.close()


    # Clear all manual resets for the new season
    manual_reset_timestamps.clear()
    print("--- Manual reset timestamps cleared for the new season. ---")
    print("--- Scheduled End of Season Job Finished ---")


# --- Database Setup & Other Listeners ---
def setup_database():
    """
    Connects to the database and creates all necessary tables for both games.
    """
    # ... (SQL commands remain the same) ...
    spots_table_command = """
    CREATE TABLE IF NOT EXISTS spots (
        id SERIAL PRIMARY KEY,
        spotter_id TEXT NOT NULL,
        spotted_id TEXT NOT NULL,
        channel_id TEXT NOT NULL,
        message_ts TEXT NOT NULL,
        image_url TEXT NOT NULL,
        spotter_points INTEGER NOT NULL DEFAULT 1,
        caught_points INTEGER NOT NULL DEFAULT 1,
        season_id TEXT NOT NULL,
        is_valid BOOLEAN NOT NULL DEFAULT TRUE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (message_ts, spotted_id)
    );
    """
    assassin_players_table_command = """
    CREATE TABLE IF NOT EXISTS assassin_players (
        id SERIAL PRIMARY KEY,
        channel_id TEXT NOT NULL,
        player_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        kill_count INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    assassin_eliminations_table_command = """
    CREATE TABLE IF NOT EXISTS assassin_eliminations (
        id SERIAL PRIMARY KEY,
        channel_id TEXT NOT NULL,
        killer_id TEXT NOT NULL,
        victim_id TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        if not db_url:
            print("🔴 DATABASE_URL is not set. Please check your .env file.")
            return
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute(spots_table_command)
        cur.execute(assassin_players_table_command)
        cur.execute(assassin_eliminations_table_command)
        conn.commit()
        print("✅ All database tables are ready.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error while connecting to PostgreSQL: {error}")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def get_user_name(user_id):
    if user_id in user_cache:
        return user_cache[user_id]
    try:
        result = app.client.users_info(user=user_id)
        user_name = result['user']['profile'].get('real_name', result['user']['profile'].get('display_name', result['user']['name']))
        user_cache[user_id] = user_name
        return user_name
    except Exception as e:
        print(f"Error fetching user info for {user_id}: {e}")
        return f"User ({user_id})"

# ... (Existing Spot Bot listeners: is_spot_message_and_not_command, handle_spot_message, handle_message_deletion)

def is_spot_message_and_not_command(message):
    text = message.get("text", "")
    has_keyword = re.search(r"\b(spot|spotted)\b", text, re.IGNORECASE)
    is_command = text.strip().startswith(f"<@{BOT_USER_ID}>")
    return has_keyword and not is_command

@app.message(matchers=[is_spot_message_and_not_command])
def handle_spot_message(message, say):
    # ... (code is unchanged) ...
    print("\n--- DEBUG: `handle_spot_message` was triggered. ---")

    if 'user' not in message or 'files' not in message or 'text' not in message:
        return

    spotter_id = message['user']
    text = message['text']
    channel_id = message['channel']

    mentioned_users = set(re.findall(r"<@(\w+)>", text))
    if not mentioned_users:
        return

    successful_spots = 0
    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        for spotted_id in mentioned_users:
            if spotter_id == spotted_id:
                continue

            spotter_points_to_award = 1
            # Check the global dict for bonus points
            if channel_id in daily_bonus_users and spotted_id in daily_bonus_users.get(channel_id, set()):
                spotter_points_to_award = 2
                print(f"--- DEBUG: Awarding 2 bonus points for spotting {spotted_id} in {channel_id}. ---")

            insert_command = """
            INSERT INTO spots (spotter_id, spotted_id, channel_id, message_ts, image_url, season_id, spotter_points, caught_points)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """

            spot_data = (
                spotter_id,
                spotted_id,
                channel_id,
                message['ts'],
                message['files'][0]['url_private'],
                get_current_season_id(),
                spotter_points_to_award,
                1 # caught_points is always 1
            )

            cur.execute(insert_command, spot_data)
            successful_spots += 1

        conn.commit()

        if successful_spots > 0:
            app.client.reactions_add(
                channel=message['channel'],
                timestamp=message['ts'],
                name="white_check_mark"
            )

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 DEBUG: An error occurred during database operation: {error}")
    finally:
         if cur is not None:
             cur.close()
         if conn is not None:
             conn.close()

@app.event({"type": "message", "subtype": "message_deleted"})
def handle_message_deletion(event):
    # ... (code is unchanged) ...
    print("\n--- DEBUG: `handle_message_deletion` (subtype) was triggered. ---")

    if 'previous_message' not in event or 'ts' not in event['previous_message']:
        print("--- DEBUG: No previous_message or ts found in deletion event. Skipping. ---")
        return

    deleted_ts = event['previous_message']['ts']
    print(f"--- DEBUG: A message with timestamp {deleted_ts} was deleted. Checking database. ---")

    conn = None
    cur = None
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

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 DEBUG: An error occurred during message deletion handling: {error}")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()


# --- Command Handlers and Listeners (Spot Bot) ---
# ... (All your existing spotboard, caughtboard, miss you, etc. handlers)

def handle_spotboard_command(message, say):
    # ... (code is unchanged) ...
    try:
        channel_id = message['channel']
        current_season = get_current_season_id()

        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = """
            SELECT spotter_id, SUM(spotter_points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND season_id = %s AND channel_id = %s
        """
        params = [current_season, channel_id]

        if channel_id in manual_reset_timestamps:
            query += " AND created_at >= %s"
            params.append(manual_reset_timestamps[channel_id])

        query += """
            GROUP BY spotter_id
            ORDER BY total_score DESC
            LIMIT 5;
        """

        cur.execute(query, tuple(params))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No spots have been recorded this season since the last reset!")
            return

        leaderboard_text = f"*Spotboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row; score = int(score); user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score}\n"

        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling spotboard command: {error}")
        say("Sorry, I had trouble fetching the spotboard.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()


def handle_caughtboard_command(message, say):
    # ... (code is unchanged) ...
    try:
        channel_id = message['channel']
        current_season = get_current_season_id()

        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = """
            SELECT spotted_id, SUM(caught_points) AS total_score
            FROM spots
            WHERE is_valid = TRUE AND season_id = %s AND channel_id = %s
        """
        params = [current_season, channel_id]

        if channel_id in manual_reset_timestamps:
            query += " AND created_at >= %s"
            params.append(manual_reset_timestamps[channel_id])

        query += """
            GROUP BY spotted_id
            ORDER BY total_score DESC
            LIMIT 5;
        """

        cur.execute(query, tuple(params))
        results = cur.fetchall()
        cur.close()
        conn.close()

        if not results:
            say("No one has been spotted this season since the last reset!")
            return

        leaderboard_text = f"*Caughtboard:*\n\n"
        for i, row in enumerate(results):
            user_id, score = row; score = int(score); user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score}\n"

        say(leaderboard_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling caughtboard command: {error}")
        say("Sorry, I had trouble fetching the caughtboard.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

def handle_alltime_spotboard_command(message, say):
    # ... (code is unchanged) ...
    try:
        channel_id = message['channel']
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        query = """
            SELECT spotter_id, SUM(spotter_points) AS total_score
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
            user_id, score = row; score = int(score); user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score}\n"
        say(leaderboard_text)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling all-time spotboard command: {error}")
        say("Sorry, I had trouble fetching the all-time spotboard.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

def handle_alltime_caughtboard_command(message, say):
    # ... (code is unchanged) ...
    try:
        channel_id = message['channel']
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        query = """
            SELECT spotted_id, SUM(caught_points) AS total_score
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
            user_id, score = row; score = int(score); user_name = get_user_name(user_id)
            leaderboard_text += f"{i+1}. {user_name} - {score}\n"
        say(leaderboard_text)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling all-time caughtboard command: {error}")
        say("Sorry, I had trouble fetching the all-time caughtboard.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

def handle_miss_you_command(message, say):
    # ... (code is unchanged) ...
    try:
        text = message.get('text', '')
        mentioned_users = re.findall(r"<@(\w+)>", text)

        if not mentioned_users:
            say("You need to tell me who you miss! Please mention a user, like `miss you @Rohan`.")
            return

        target_user_id = mentioned_users[0]
        channel_id = message['channel']

        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        query = "SELECT image_url FROM spots WHERE spotted_id = %s AND channel_id = %s AND is_valid = TRUE"
        cur.execute(query, (target_user_id, channel_id))

        image_urls = [row[0] for row in cur.fetchall()]

        cur.close()
        conn.close()

        if not image_urls:
            target_user_name = get_user_name(target_user_id)
            say(f"Sorry, I couldn't find any pictures of {target_user_name} in this channel.")
            return

        random_image_url = random.choice(image_urls)
        target_user_name = get_user_name(target_user_id)

        say(f"Missing them? Here's a memory of {target_user_name}!\n{random_image_url}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling 'miss you' command: {error}")
        say("Sorry, I had a problem fetching that picture.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

def handle_mystats_command(message, say):
    # ... (code is unchanged) ...
    try:
        user_id = message['user']
        channel_id = message['channel']

        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # 1. Get total spots made by the user
        cur.execute("SELECT SUM(spotter_points) FROM spots WHERE spotter_id = %s AND channel_id = %s AND is_valid = TRUE", (user_id, channel_id))
        spots_made = cur.fetchone()[0] or 0

        # 2. Get total times the user was caught
        cur.execute("SELECT SUM(caught_points) FROM spots WHERE spotted_id = %s AND channel_id = %s AND is_valid = TRUE", (user_id, channel_id))
        times_caught = cur.fetchone()[0] or 0

        # 3. Get the user's most frequent target
        cur.execute("""
            SELECT spotted_id, COUNT(*) as spot_count
            FROM spots
            WHERE spotter_id = %s AND channel_id = %s AND is_valid = TRUE
            GROUP BY spotted_id
            ORDER BY spot_count DESC
            LIMIT 1;
        """, (user_id, channel_id))
        nemesis_result = cur.fetchone()

        cur.close()
        conn.close()

        user_name = get_user_name(user_id)
        stats_text = f"📊 *{user_name}'s Spotting Record in this channel:*\n\n"
        stats_text += f"• You have spotted others *{int(spots_made)}* times.\n"
        stats_text += f"• You have been spotted *{int(times_caught)}* times.\n"

        if nemesis_result:
            nemesis_id, nemesis_count = nemesis_result
            nemesis_name = get_user_name(nemesis_id)
            stats_text += f"• Your most frequent target is *{nemesis_name}* ({nemesis_count} spots)."
        else:
            stats_text += "• You haven't spotted anyone yet!"

        say(stats_text)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error handling mystats command: {error}")
        say("Sorry, I had trouble fetching your stats.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

def handle_explode_command(message, say, client):
    # ... (code is unchanged) ...
    try:
        text = message.get('text', '')
        mentioned_users = re.findall(r"<@(\w+)>", text)

        if not mentioned_users:
            say("You need to tell me who to explode! Please mention a user, like `explode @Rohan`.")
            return

        target_user_id = mentioned_users[0]
        channel_id = message['channel']

        # 1. Find a random image URL from the database
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        query = "SELECT image_url FROM spots WHERE spotted_id = %s AND channel_id = %s AND is_valid = TRUE"
        cur.execute(query, (target_user_id, channel_id))
        image_urls = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        if not image_urls:
            target_user_name = get_user_name(target_user_id)
            say(f"Sorry, I couldn't find any pictures of {target_user_name} to explode.")
            return

        random_image_url = random.choice(image_urls)

        # 2. Download the user image and select a random local explosion image
        auth_header = {"Authorization": f"Bearer {os.environ.get('SLACK_BOT_TOKEN')}"}
        user_image_response = requests.get(random_image_url, headers=auth_header)
        user_image_response.raise_for_status()

        try:
            explosion_files = [f for f in os.listdir(EXPLOSIONS_DIR) if f.lower().endswith('.png')]
            if not explosion_files:
                say("I couldn't find any explosion images in my folder!")
                return
            random_explosion_path = os.path.join(EXPLOSIONS_DIR, random.choice(explosion_files))
        except FileNotFoundError:
            print(f"🔴 Error: The directory '{EXPLOSIONS_DIR}' was not found.")
            say("I'm having trouble finding my explosion effects. Please check my configuration.")
            return

        # 3. Process the images with Pillow
        base_image = Image.open(io.BytesIO(user_image_response.content)).convert("RGBA")
        explosion_image = Image.open(random_explosion_path).convert("RGBA")

        # Resize explosion to match the base image size
        explosion_image = explosion_image.resize(base_image.size)

        # Composite the images
        composite_image = Image.alpha_composite(base_image, explosion_image)

        # Save the result to a temporary in-memory file
        temp_file = io.BytesIO()
        composite_image.save(temp_file, format='PNG')
        temp_file.seek(0)

        # 4. Upload the new image to Slack
        target_user_name = get_user_name(target_user_id)
        client.files_upload_v2(
            channel=channel_id,
            initial_comment=f"💥 {target_user_name} has been exploded! 💥",
            file=temp_file,
            filename="explosion.png"
        )

    except Exception as e:
        print(f"🔴 Error in explode command: {e}")
        say("Sorry, I had trouble creating the explosion. The image might be too powerful.")
    finally: # Ensure connection is closed
        if 'cur' in locals() and cur is not None and not cur.closed:
             cur.close()
        if 'conn' in locals() and conn is not None and not conn.closed:
             conn.close()

# --- Assassin Game Command Handlers ---

def handle_assassin_start_command(message, say, client):
    # ... (Admin check and start logic remains the same) ...
    channel_id = message['channel']
    starter_id = message['user']
    text = message.get('text', '')

    # --- ADMIN CHECK ---
    if starter_id != ADMIN_USER_ID:
        client.chat_postEphemeral(
            channel=channel_id,
            user=starter_id,
            text="Sorry, only the designated admin can start an Assassin game."
        )
        return
    # --- END ADMIN CHECK ---

    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # 1. Check if a game is already running in this channel
        cur.execute("SELECT COUNT(*) FROM assassin_players WHERE channel_id = %s AND is_active = TRUE", (channel_id,))
        active_game_count = cur.fetchone()[0]
        if active_game_count > 0:
            say("An Assassin game is already in progress in this channel! Use `assassin end` to stop it first.")
            return

        # 2. Gather players
        mentioned_users = list(set(re.findall(r"<@(\w+)>", text)))
        if len(mentioned_users) < 3:
            say("You need at least 3 players to start a game of Assassin. Please mention everyone who is playing.")
            return

        # 3. Clear old game data for the channel and shuffle players
        cur.execute("DELETE FROM assassin_players WHERE channel_id = %s", (channel_id,))
        cur.execute("DELETE FROM assassin_eliminations WHERE channel_id = %s", (channel_id,))

        players = mentioned_users
        random.shuffle(players)

        # 4. Assign targets and insert into database
        for i, player_id in enumerate(players):
            target_id = players[(i + 1) % len(players)] # The next player in the shuffled list
            cur.execute(
                "INSERT INTO assassin_players (channel_id, player_id, target_id) VALUES (%s, %s, %s)",
                (channel_id, player_id, target_id)
            )

        conn.commit()

        # 5. Announce the game start and notify players of their targets privately via DM
        player_names = ", ".join([f"<@{p}>" for p in players])
        say(f"A new game of Assassin has begun!\n*Players:* {player_names}\nEach player has been sent their first target via DM. Good luck!")

        print(f"--- Attempting to send targets for channel {channel_id} via DM ---")
        for player_id in players:
            try:
                cur.execute("SELECT target_id FROM assassin_players WHERE player_id = %s AND channel_id = %s", (player_id, channel_id))
                target_id_result = cur.fetchone()
                if not target_id_result:
                    print(f"🔴 DEBUG: Could not find target_id for player {player_id} in DB.")
                    continue # Skip this player if DB fetch failed

                target_id = target_id_result[0]
                target_name = get_user_name(target_id)
                print(f"--- DEBUG: Preparing DM for player {player_id} ({get_user_name(player_id)}) their target is {target_id} ({target_name}) ---")

                client.chat_postMessage(
                    channel=player_id, # Send to the user directly
                    text=f"Your first Assassin target in the <#{channel_id}> channel is: *{target_name}*."
                )
                print(f"--- DEBUG: Successfully sent DM to {player_id} ---")
            except Exception as e:
                print(f"🔴 DEBUG: Error sending DM to {player_id}: {e}")
                starter_name = get_user_name(starter_id)
                failed_player_name = get_user_name(player_id)
                say(f"⚠️ {starter_name}, I couldn't send a DM to {failed_player_name}. They might need to check their app permissions or start a conversation with me first.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_start_command: {error}")
        say("Sorry, I ran into an error trying to start the game.")
    finally:
        # Ensure connection is closed
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_target_command(message, say, client):
    # ... (code is unchanged) ...
    channel_id = message['channel']
    player_id = message['user']

    conn = None
    cur = None
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute("SELECT target_id, is_active FROM assassin_players WHERE player_id = %s AND channel_id = %s", (player_id, channel_id))
        result = cur.fetchone()

        if not result:
            client.chat_postEphemeral(channel=channel_id, user=player_id, text="You are not currently in a game of Assassin in this channel.")
            return

        target_id, is_active = result
        if not is_active:
            client.chat_postEphemeral(channel=channel_id, user=player_id, text="You have been eliminated from the game!")
            return

        target_name = get_user_name(target_id)
        client.chat_postEphemeral(channel=channel_id, user=player_id, text=f"Your current target is: *{target_name}*.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_target_command: {error}")
        client.chat_postEphemeral(channel=channel_id, user=player_id, text="Sorry, I had a problem fetching your target.")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()


def handle_eliminated_command(message, say, client):
    # ... (code is unchanged, including the fix to ignore self-messages) ...
    print(f"\n--- DEBUG: handle_eliminated_command triggered by message: {message.get('text', '')[:50]} ---")
    print(f"--- DEBUG: Message user: {message.get('user')}, Bot ID: {BOT_USER_ID}, Message has bot_id: {'bot_id' in message} ---")

    # More robust check: Ignore messages sent by the bot itself OR any other bot
    if message.get('user') == BOT_USER_ID or message.get('bot_id') is not None:
        print("--- DEBUG: Ignoring message from self or another bot in handle_eliminated_command ---")
        return

    channel_id = message['channel']
    # Ensure 'user' exists before using it, though the check above should handle most cases
    killer_id = message.get('user')
    if not killer_id:
        print("--- DEBUG: Message missing 'user' field in handle_eliminated_command. Skipping. ---")
        return # Cannot process if we don't know who sent it

    text = message.get('text', '')

    # 1. Basic Validation
    if 'files' not in message:
        say("An elimination attempt requires photo or video proof!")
        return

    mentioned_users = re.findall(r"<@(\w+)>", text)
    if not mentioned_users:
        say("You must mention the player you are eliminating.")
        return
    victim_id = mentioned_users[0]

    conn = None # Initialize outside try
    cur = None # Initialize outside try
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()

        # 2. Advanced Validation
        cur.execute("SELECT target_id, is_active FROM assassin_players WHERE player_id = %s AND channel_id = %s", (killer_id, channel_id))
        killer_data = cur.fetchone()

        # This check should now only run for actual user messages
        if not killer_data:
            say("You are not a player in the current game.")
            return

        killer_target, killer_is_active = killer_data
        if not killer_is_active:
            say("You can't eliminate someone when you've already been eliminated!")
            return

        if killer_target != victim_id:
            say("That is not your target!")
            return

        # Check if victim exists and is active
        cur.execute("SELECT target_id, is_active FROM assassin_players WHERE player_id = %s AND channel_id = %s", (victim_id, channel_id))
        victim_data = cur.fetchone()
        if not victim_data or not victim_data[1]: # If victim doesn't exist or is already inactive
             say("Your target has already been eliminated.")
             return

        # 3. Process the elimination
        new_target_id = victim_data[0] # Get the victim's target

        # Update victim's status
        cur.execute("UPDATE assassin_players SET is_active = FALSE WHERE player_id = %s AND channel_id = %s", (victim_id, channel_id))

        # Update killer's status
        cur.execute("UPDATE assassin_players SET target_id = %s, kill_count = kill_count + 1 WHERE player_id = %s AND channel_id = %s", (new_target_id, killer_id, channel_id))

        # Log the elimination
        cur.execute("INSERT INTO assassin_eliminations (channel_id, killer_id, victim_id) VALUES (%s, %s, %s)", (channel_id, killer_id, victim_id))

        conn.commit()

        # 4. Check for a winner
        cur.execute("SELECT player_id FROM assassin_players WHERE channel_id = %s AND is_active = TRUE", (channel_id,))
        active_players = cur.fetchall()

        killer_name = get_user_name(killer_id)
        victim_name = get_user_name(victim_id)

        if len(active_players) == 1:
            winner_id = active_players[0][0]
            winner_name = get_user_name(winner_id)
            say(f"💥 *{killer_name}* has eliminated *{victim_name}*! 💥\n\n🏆 The game is over! Congratulations to the winner, *{winner_name}*! 🏆")
            # Clear the game board - Consider just marking as inactive? For now, deleting.
            cur.execute("DELETE FROM assassin_players WHERE channel_id = %s", (channel_id,))
            conn.commit()
        else:
            # Announce elimination and notify killer of new target
            say(f"💥 *{killer_name}* has eliminated *{victim_name}*! 💥")
            new_target_name = get_user_name(new_target_id)
            client.chat_postEphemeral(
                channel=channel_id,
                user=killer_id,
                text=f"Congratulations on the elimination! Your new target is: *{new_target_name}*."
            )

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in eliminated_command: {error}")
        say("Sorry, I encountered an error while processing the elimination.")
    finally:
        # Ensure connection is closed even if errors occur before explicit close
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()


def handle_assassin_alive_command(message, say):
    # ... (code is unchanged) ...
    channel_id = message['channel']
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("SELECT player_id FROM assassin_players WHERE channel_id = %s AND is_active = TRUE ORDER BY created_at", (channel_id,))
        active_players_ids = [row[0] for row in cur.fetchall()]


        if not active_players_ids:
            say("No game is currently active, or everyone has been eliminated!")
            return

        alive_list = "\n".join([f"• {get_user_name(pid)}" for pid in active_players_ids])
        say(f"Players still alive:\n{alive_list}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_alive_command: {error}")
        say("Sorry, I couldn't fetch the list of active players.")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_dead_command(message, say):
    # ... (code is unchanged) ...
    channel_id = message['channel']
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()
        # Fetching eliminated players along with who eliminated them and when
        cur.execute("""
            SELECT ap.player_id, ae.killer_id, ae.created_at
            FROM assassin_players ap
            LEFT JOIN assassin_eliminations ae ON ap.player_id = ae.victim_id AND ap.channel_id = ae.channel_id
            WHERE ap.channel_id = %s AND ap.is_active = FALSE
            ORDER BY ae.created_at DESC NULLS LAST
            """, (channel_id,))
        eliminated_players_data = cur.fetchall()


        if not eliminated_players_data:
            say("No players have been eliminated yet in this game.")
            return

        dead_list_lines = []
        for victim_id, killer_id, eliminated_at in eliminated_players_data:
            victim_name = get_user_name(victim_id)
            if killer_id and eliminated_at:
                 killer_name = get_user_name(killer_id)
                 eliminated_at_str = eliminated_at.strftime("%Y-%m-%d %H:%M")
                 dead_list_lines.append(f"• {victim_name} (eliminated by {killer_name} on {eliminated_at_str})")
            else:
                 # Should ideally not happen if data is consistent, but handles edge cases
                 dead_list_lines.append(f"• {victim_name} (Eliminated)")

        dead_list = "\n".join(dead_list_lines)
        say(f"Players who have been eliminated:\n{dead_list}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_dead_command: {error}")
        say("Sorry, I couldn't fetch the list of eliminated players.")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_killcount_command(message, say):
    # ... (code is unchanged) ...
    channel_id = message['channel']
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("""
            SELECT player_id, kill_count
            FROM assassin_players
            WHERE channel_id = %s AND kill_count > 0
            ORDER BY kill_count DESC
            LIMIT 3
            """, (channel_id,))
        top_killers = cur.fetchall()


        if not top_killers:
            say("No kills have been recorded yet in this game.")
            return

        killboard_lines = []
        for i, (player_id, kill_count) in enumerate(top_killers):
            player_name = get_user_name(player_id)
            killboard_lines.append(f"{i+1}. {player_name} - {kill_count} kills")

        killboard_text = "\n".join(killboard_lines)
        say(f"*Assassin Killboard (Top 3):*\n{killboard_text}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_killcount_command: {error}")
        say("Sorry, I couldn't fetch the killboard.")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_end_request(message, client, say):
    # ... (Admin check added previously) ...
    channel_id = message['channel']
    user_id = message['user']

    # --- ADMIN CHECK ---
    if user_id != ADMIN_USER_ID:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="Sorry, only the designated admin can end an Assassin game."
        )
        return
    # --- END ADMIN CHECK ---

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM assassin_players WHERE channel_id = %s AND is_active = TRUE", (channel_id,))
        active_game_count = cur.fetchone()[0]

        if active_game_count == 0:
            say("There is no active Assassin game in this channel to end.")
            return

        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="Are you sure you want to end the current Assassin game? This cannot be undone.",
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "Are you sure you want to end the current Assassin game? This will clear all game data for this channel."}}, {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "Confirm End Game"}, "style": "danger", "action_id": "confirm_end_assassin_action"}, {"type": "button", "text": {"type": "plain_text", "text": "Cancel"}, "action_id": "cancel_end_assassin_action"}]}]
        )
    except Exception as e:
        print(f"🔴 Error sending end game confirmation: {e}")
        say("Sorry, I couldn't process the request to end the game.")
    finally:
        # Ensure connection is closed even if errors occur before explicit close
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_targets_command(message, client):
    """Admin command to DM the list of current targets."""
    channel_id = message['channel']
    user_id = message['user']

    # --- ADMIN CHECK ---
    if user_id != ADMIN_USER_ID:
        client.chat_postEphemeral(
            channel=channel_id,
            user=user_id,
            text="Sorry, this is an admin-only command."
        )
        return
    # --- END ADMIN CHECK ---

    conn = None
    cur = None
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()

        cur.execute("""
            SELECT player_id, target_id
            FROM assassin_players
            WHERE channel_id = %s AND is_active = TRUE
            ORDER BY created_at
            """, (channel_id,))
        targets = cur.fetchall()

        if not targets:
             client.chat_postMessage(channel=user_id, text=f"No active Assassin game found in <#{channel_id}>.")
             return

        target_list_lines = [f"*Current Assassin Targets in <#{channel_id}>:*"]
        for player_id, target_id in targets:
            player_name = get_user_name(player_id)
            target_name = get_user_name(target_id)
            target_list_lines.append(f"• {player_name} is targeting {target_name}")

        target_list_text = "\n".join(target_list_lines)
        client.chat_postMessage(channel=user_id, text=target_list_text) # Send DM to admin

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in assassin_targets_command: {error}")
        client.chat_postMessage(channel=user_id, text="Sorry, I encountered an error fetching the target list.")
    finally:
        if cur is not None:
             cur.close()
        if conn is not None:
             conn.close()

def handle_assassin_help_command(message, say):
    """Displays the help message for the Assassin game."""
    help_text = """
*Assassin Game Commands:*
• `assassin target` or `mytarget`: Privately shows you your current target.
• `eliminated @target` (with image/video): Report that you have eliminated your target.
• `assassin alive`: Shows a list of players still in the game.
• `assassin dead`: Shows a list of eliminated players.
• `assassin killcount`: Displays the top 3 players by number of eliminations.
_Admin commands (`assassin start`, `assassin end`, `assassin targets`) are restricted._
"""
    say(help_text)

def handle_spot_help_command(message, say):
    """Displays the help message for the Spot Bot game."""
    help_text = """
*Spot Bot Commands:*
• `spot @user` or `spotted @user` (with image): Record a spot. Counts for 1 point (or 2 for daily bonus targets!).
• `spotboard`: Show the seasonal leaderboard of top spotters.
• `caughtboard`: Show the seasonal leaderboard of most spotted players.
• `alltimespotboard`: Show the all-time leaderboard of top spotters.
• `alltimecaughtboard`: Show the all-time leaderboard of most spotted players.
• `reset`: Manually end the current season and start a new one (admin only).
• `miss you @user` or `i miss u @user`: Shows a random past spot picture of the mentioned user.
• `mystats`: Shows your personal spotting stats in this channel.
• `explode @user`: Overlays a random explosion on a random spot picture of the mentioned user.
• `dailybonus`: Shows who the current daily bonus targets are.
• `assassin help`: Show commands for the Assassin game.
"""
    say(help_text)

def handle_daily_bonus_command(message, say):
    """Displays the current daily bonus targets for the channel."""
    channel_id = message['channel']
    if channel_id in daily_bonus_users and daily_bonus_users[channel_id]:
        targets = list(daily_bonus_users[channel_id])
        user1_name = get_user_name(targets[0])
        user2_name = get_user_name(targets[1])
        say(f"Today's bonus targets are *{user1_name}* and *{user2_name}*! Spots of them are worth 2 points.")
    else:
        say("Bonus targets haven't been assigned for today yet, or this channel isn't active in the Spot Bot game.")


# --- Keyword Listeners ---

# ... (Existing Spot Bot listeners)
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

@app.message(re.compile(r"^reset$", re.IGNORECASE))
def handle_reset_request(message, client):
    # --- ADMIN CHECK ---
    if message['user'] != ADMIN_USER_ID:
        client.chat_postEphemeral(
            channel=message['channel'],
            user=message['user'],
            text="Sorry, only the designated admin can reset the Spot Bot season."
        )
        return
    # --- END ADMIN CHECK ---
    try:
        client.chat_postEphemeral(
            channel=message['channel'],
            user=message['user'],
            text="Are you sure you want to reset the seasonal leaderboards? This cannot be undone.",
            blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "Are you sure you want to reset the seasonal leaderboards? This will announce the winner of the current interim season and start a fresh board."}}, {"type": "actions", "elements": [{"type": "button", "text": {"type": "plain_text", "text": "Confirm Reset"}, "style": "danger", "action_id": "confirm_reset_action"}, {"type": "button", "text": {"type": "plain_text", "text": "Cancel"}, "action_id": "cancel_reset_action"}]}]
        )
    except Exception as e:
        print(f"🔴 Error sending reset confirmation: {e}")

@app.message(re.compile(r"^(i miss (you|u)|miss (you|u))", re.IGNORECASE))
def handle_miss_you_keyword(message, say):
    handle_miss_you_command(message, say)

@app.message(re.compile(r"^mystats$", re.IGNORECASE))
def handle_mystats_keyword(message, say):
    handle_mystats_command(message, say)

@app.message(re.compile(r"^explode", re.IGNORECASE))
def handle_explode_keyword(message, say, client):
    handle_explode_command(message, say, client)

@app.message(re.compile(r"^help$", re.IGNORECASE))
def handle_spot_help_keyword(message, say):
    handle_spot_help_command(message, say)

@app.message(re.compile(r"^dailybonus$", re.IGNORECASE))
def handle_daily_bonus_keyword(message, say):
    handle_daily_bonus_command(message, say)

# Assassin Game Keyword Listeners
@app.message(re.compile(r"^assassin start", re.IGNORECASE))
def handle_assassin_start_keyword(message, say, client):
    handle_assassin_start_command(message, say, client)

@app.message(re.compile(r"^(assassin target|mytarget)$", re.IGNORECASE))
def handle_assassin_target_keyword(message, say, client):
    handle_assassin_target_command(message, say, client)

@app.message(re.compile(r"^(eliminated|eliminate)", re.IGNORECASE))
def handle_eliminated_keyword(message, say, client):
    # This might need adjustment if "eliminate" is used elsewhere
    # Check if the message context implies the assassin game.
    # For now, assuming any "eliminated" refers to assassin.
    handle_eliminated_command(message, say, client)

@app.message(re.compile(r"^assassin alive$", re.IGNORECASE))
def handle_assassin_alive_keyword(message, say):
    handle_assassin_alive_command(message, say)

@app.message(re.compile(r"^assassin dead$", re.IGNORECASE))
def handle_assassin_dead_keyword(message, say):
    handle_assassin_dead_command(message, say)

@app.message(re.compile(r"^assassin killcount$", re.IGNORECASE))
def handle_assassin_killcount_keyword(message, say):
    handle_assassin_killcount_command(message, say)

@app.message(re.compile(r"^assassin end$", re.IGNORECASE))
def handle_assassin_end_keyword(message, client, say):
    handle_assassin_end_request(message, client, say)

@app.message(re.compile(r"^assassin targets$", re.IGNORECASE))
def handle_assassin_targets_keyword(message, client):
    handle_assassin_targets_command(message, client)

@app.message(re.compile(r"^assassin help$", re.IGNORECASE))
def handle_assassin_help_keyword(message, say):
    handle_assassin_help_command(message, say)


# --- Action (Button Click) Listeners ---
# ... (Existing reset listeners)
@app.action("confirm_reset_action")
def handle_confirm_reset_action(ack, body, client):
    """
    Handles the confirmation of a manual season reset.
    Announces the winner of the interim period and sets a new reset timestamp.
    """
    ack()
    global manual_reset_timestamps

    try:
        channel_id = body['channel']['id']
        season_to_end_id = get_current_season_id()
        announce_season_winner(season_to_end_id, channel_id, is_manual_reset=True)

        manual_reset_timestamps[channel_id] = datetime.now(pytz.timezone('America/Los_Angeles'))
        print(f"--- MANUAL RESET: Reset timestamp set for channel {channel_id} ---")

        client.chat_delete(
            channel=body['channel']['id'],
            ts=body['message']['ts']
        )
    except Exception as e:
        print(f"🔴 Error in confirm_reset_action: {e}")

@app.action("cancel_reset_action")
def handle_cancel_reset_action(ack, body, client):
    ack()
    try:
        client.chat_delete(
            channel=body['channel']['id'],
            ts=body['message']['ts']
        )
    except Exception as e:
        print(f"🔴 Error in cancel_reset_action: {e}")

@app.action("confirm_end_assassin_action")
def handle_confirm_end_action(ack, body, client, say):
    """Handles the confirmation to end the Assassin game."""
    ack() # Acknowledge the action immediately

    channel_id = body['channel']['id']
    user_id = body['user']['id'] # Get the user who clicked the button
    # Correct way to get ts for ephemeral message actions
    message_ts = body['container']['message_ts']

    conn = None # Initialize conn outside try
    cur = None # Initialize cur outside try
    try:
        conn = psycopg2.connect(os.environ.get("DATABASE_URL"))
        cur = conn.cursor()

        print(f"--- DEBUG: Attempting to DELETE game data for channel {channel_id} ---")
        cur.execute("DELETE FROM assassin_players WHERE channel_id = %s", (channel_id,))
        players_deleted = cur.rowcount
        cur.execute("DELETE FROM assassin_eliminations WHERE channel_id = %s", (channel_id,))
        eliminations_deleted = cur.rowcount

        conn.commit()
        print(f"--- DEBUG: DELETEd {players_deleted} players and {eliminations_deleted} eliminations ---")


        say(f"🛑 The Assassin game in this channel has been manually ended by <@{user_id}>.")
        print(f"--- ASSASSIN GAME ENDED in channel {channel_id} by user {user_id} ---")

        # Delete the original ephemeral confirmation message
        # We put this *after* the critical DB operations
        client.chat_delete(
            channel=channel_id,
            ts=message_ts
        )

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in confirm_end_assassin_action: {error}")
        # Try to inform the user even if the main action failed
        # No need to post another error message here if deletion fails
    finally:
        # Ensure the database connection is always closed
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()
        print("--- DEBUG: Database connection closed in confirm_end_assassin_action ---")


@app.action("cancel_end_assassin_action")
def handle_cancel_end_action(ack, body, client):
    """Handles the cancellation of ending the Assassin game."""
    ack() # Acknowledge the action immediately

    channel_id = body['channel']['id']
    # Correct way to get ts for ephemeral message actions
    message_ts = body['container']['message_ts']
    try:
        # We still want to delete the message if the user clicks Cancel
        client.chat_delete(
            channel=channel_id,
            ts=message_ts # Use the correctly retrieved timestamp
        )
    except Exception as e:
        print(f"🔴 Error in cancel_end_assassin_action: {e}")


@app.event("app_mention")
def handle_mention(event, say, client):
    """
    Handles commands when the bot is @-mentioned.
    """
    command_text = event['text'].strip().lower()
    # Extract the actual text after the mention
    # Example: "<@BOTID> help" -> "help"
    # Example: "<@BOTID>" -> ""
    command_part = re.sub(r'^<@\w+>\s*', '', command_text).strip()

    # Assassin game commands via mention
    if command_part.startswith("assassin start"):
        handle_assassin_start_command(event, say, client)
    elif command_part == "assassin target" or command_part == "mytarget":
        handle_assassin_target_command(event, say, client)
    elif command_part.startswith("eliminated") or command_part.startswith("eliminate"):
        handle_eliminated_command(event, say, client)
    elif command_part == "assassin alive":
         handle_assassin_alive_command(event, say)
    elif command_part == "assassin dead":
         handle_assassin_dead_command(event, say)
    elif command_part == "assassin killcount":
         handle_assassin_killcount_command(event, say)
    elif command_part == "assassin end":
         handle_assassin_end_request(event, client, say)
    elif command_part == "assassin targets": # New admin command
         handle_assassin_targets_command(event, client)
    elif command_part == "assassin help": # New help command
         handle_assassin_help_command(event, say)
    # Existing Spot Bot commands via mention
    elif command_part.startswith("explode"):
        handle_explode_command(event, say, client)
    elif command_part == "mystats":
        handle_mystats_command(event, say)
    elif command_part.startswith("miss"): # Matches "miss you", "miss u" etc.
        handle_miss_you_command(event, say)
    elif command_part == "alltimecaughtboard" or command_part == "all time caught board":
        handle_alltime_caughtboard_command(event, say)
    elif command_part == "alltimespotboard" or command_part == "all time spot board":
        handle_alltime_spotboard_command(event, say)
    elif command_part == "caughtboard":
        handle_caughtboard_command(event, say)
    elif command_part == "spotboard":
        handle_spotboard_command(event, say)
    elif command_part == "test bonus":
        say("Sure, I'll run the daily bonus job for you right now. Check the channel for an announcement if it's eligible.")
        daily_bonus_job()
    elif command_part == "dailybonus": # New command
        handle_daily_bonus_command(event, say)
    elif command_part == "help" or command_part == "": # Check for just "help" or only the mention
        handle_spot_help_command(event, say)
    else: # Default fallback if no other command matches
        handle_spot_help_command(event, say) # Show general help by default


# --- Main Application Execution ---
if __name__ == "__main__":
    setup_database()

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

