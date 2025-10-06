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
        
        cur.close()
        conn.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error in announce_season_winner: {error}")


# --- Scheduled Job Functions ---
def daily_bonus_job():
    """
    Selects two random users per active channel to be bonus targets for the day.
    """
    print("--- Running Daily Bonus Job ---")
    global daily_bonus_users
    daily_bonus_users.clear()

    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT channel_id FROM spots")
        active_channels = [row[0] for row in cur.fetchall()]

        for channel_id in active_channels:
            cur.execute("""
                SELECT spotter_id FROM spots WHERE channel_id = %s
                UNION
                SELECT spotted_id FROM spots WHERE channel_id = %s
            """, (channel_id, channel_id))
            
            participants = [row[0] for row in cur.fetchall()]
            
            if len(participants) >= 2:
                bonus_targets = random.sample(participants, 2)
                daily_bonus_users[channel_id] = set(bonus_targets)
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
    Scheduled job that runs automatically. It determines the previous season and announces the winner.
    """
    print("--- Running Scheduled End of Season Job ---")
    global manual_reset_timestamps
    
    current_season_start_str = get_current_season_id()
    current_season_start_dt = datetime.strptime(current_season_start_str, '%Y-%m-%d').astimezone(pytz.timezone('America/Los_Angeles'))
    previous_season_start_dt = current_season_start_dt - timedelta(days=14)
    previous_season_id = previous_season_start_dt.strftime('%Y-%m-%d')
    
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT channel_id FROM spots WHERE season_id = %s", (previous_season_id,))
        channels = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        for channel_id in channels:
            announce_season_winner(previous_season_id, channel_id, is_manual_reset=False)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"🔴 Error getting channels in end_of_season_job: {error}")

    # Clear all manual resets for the new season
    manual_reset_timestamps.clear()
    print("--- Manual reset timestamps cleared for the new season. ---")
    print("--- Scheduled End of Season Job Finished ---")


# --- Database Setup & Other Listeners ---
def setup_database():
    """
    Connects to the database and creates the 'spots' table with separate point columns.
    """
    create_table_command = """
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

def is_spot_message_and_not_command(message):
    text = message.get("text", "")
    has_keyword = re.search(r"\b(spot|spotted)\b", text, re.IGNORECASE)
    is_command = text.strip().startswith(f"<@{BOT_USER_ID}>")
    return has_keyword and not is_command

@app.message(matchers=[is_spot_message_and_not_command])
def handle_spot_message(message, say):
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
    try:
        db_url = os.environ.get("DATABASE_URL")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        for spotted_id in mentioned_users:
            if spotter_id == spotted_id:
                continue
            
            spotter_points_to_award = 1
            if channel_id in daily_bonus_users and spotted_id in daily_bonus_users[channel_id]:
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

@app.event({"type": "message", "subtype": "message_deleted"})
def handle_message_deletion(event):
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
    Generates and posts the seasonal spotboard, respecting manual resets.
    """
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

def handle_caughtboard_command(message, say):
    """
    Generates and posts the seasonal caughtboard, respecting manual resets.
    """
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

def handle_alltime_spotboard_command(message, say):
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

def handle_alltime_caughtboard_command(message, say):
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

def handle_miss_you_command(message, say):
    """
    Finds a random picture of a mentioned user and posts it.
    """
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

def handle_mystats_command(message, say):
    """
    Calculates and displays personal stats for the user who sent the command.
    """
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

def handle_explode_command(message, say, client):
    """
    Finds a random picture of a user, overlays a random explosion, and uploads it.
    """
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

# --- Action (Button Click) Listeners ---
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

@app.event("app_mention")
def handle_mention(event, say, client):
    """
    Handles leaderboard commands when the bot is @-mentioned.
    """
    command_text = event['text'].strip().lower()
    
    if "explode" in command_text:
        handle_explode_command(event, say, client)
    elif "mystats" in command_text:
        handle_mystats_command(event, say)
    elif "miss" in command_text:
        handle_miss_you_command(event, say)
    elif "alltimecaughtboard" in command_text or "all time caught board" in command_text:
        handle_alltime_caughtboard_command(event, say)
    elif "alltimespotboard" in command_text or "all time spot board" in command_text:
        handle_alltime_spotboard_command(event, say)
    elif "caughtboard" in command_text:
        handle_caughtboard_command(event, say)
    elif "spotboard" in command_text:
        handle_spotboard_command(event, say)
    elif "test bonus" in command_text:
        say("Sure, I'll run the daily bonus job for you right now. Check the channel for an announcement if it's eligible.")
        daily_bonus_job()
    else:
        say(f"Hi there, <@{event['user']}>! Try one of our leaderboard commands: `spotboard`, `caughtboard`, `alltimespotboard`, or `alltimecaughtboard`.")


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

