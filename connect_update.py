
import argparse
import pyperclip
import sys
from config import DB_NAME, DB_USER, DB_PW, DB_HOST, DB_PORT
from psycopg2 import pool
from psycopg2 import DatabaseError, IntegrityError, OperationalError
from datetime import datetime
import logging
from db_utils import setup_connection_pool_with_retry, check_and_install

logging.basicConfig(level=logging.DEBUG)

def compare_data(old_data, new_data, force_include_keys=[]):
    updated_data = {}

    for key, value in new_data.items():
        # Convert datetime objects to date if necessary
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(old_data.get(key), datetime):
            old_data[key] = old_data[key].date()

        # Handle the string comparison with stripping whitespace
        old_value = old_data.get(key)
        if isinstance(value, str) and isinstance(old_value, str):
            if value.strip() != old_value.strip():
                updated_data[key] = value
                continue

        # If the key is in force_include_keys or the values are different, include it in the updated_data
        elif key in force_include_keys or old_value != value:
            updated_data[key] = value

    return updated_data

def parse_arguments():
    parser = argparse.ArgumentParser(description='Receive variables from AppleScript.')

    # Add arguments
    parser.add_argument('--curator', type=str, required=True)
    parser.add_argument('--user-id', type=str, required=True)
    parser.add_argument('--first-name', type=str, required=True)
    parser.add_argument('--follow-up-status', type=str, required=True)
    parser.add_argument('--fb', type=str, required=True)
    parser.add_argument('--fb-account', type=str, required=True)
    parser.add_argument('--ig', type=str, required=True)
    parser.add_argument('--ig-account', type=str, required=True)
    parser.add_argument('--last-contacted', type=lambda s: datetime.strptime(s, '%m/%d/%Y'), required=True)
    parser.add_argument('--language', type=str, default='ENG')
    parser.add_argument('--email', type=str, default=None)
    parser.add_argument('--source-id', type=str, default=None)
    parser.add_argument('--followers', type=int, default=None)
    parser.add_argument('--campaignid', type=int, default=None)
    parser.add_argument('--playlist-peers', type=str, default=None)
    parser.add_argument('--placement-status', type=str, default='Not yet')
    parser.add_argument('--num-messages', type=int, default=1)
    parser.add_argument('--linkedin', type=str, default=None)


    args = parser.parse_args()
    return args

def organize_args_data(args):
    # Organizing the arguments into dictionaries
    playlister_info = {
        'curatorfullname': args.curator,
        'spotifyuserid': args.user_id,
        'firstname': args.first_name,
        'facebook': args.fb,
        'followupstatus': args.follow_up_status,
        'fbcontactedby': args.fb_account,
        'instagram': args.ig,
        'igcontactedby': args.ig_account,
        'linkedin': args.linkedin,
        'lastcontacted': args.last_contacted,
        'followupstatus': 'FU3',
        'preferredlanguage': args.language,
        'email': args.email
    }

    playlist_info = None
    campaign_info = None
    if args.source_id:
        playlist_info = {
            'playlistspotifyid': args.source_id,
            'numberoffollowers': args.followers
        }
        campaign_info = {
            'campaignid': args.campaignid,
            'placementstatus': args.placement_status,
            'numberofmessages': args.num_messages,
            'referenceartists': args.playlist_peers
        }

    return playlister_info, playlist_info, campaign_info

def get_playlister_details(spotifyuserid, conn):
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT *
            FROM playlisters
            WHERE spotifyuserid = %s
        """, (spotifyuserid,))

        playlister_row = cur.fetchone()
        if not playlister_row:
            return None, None

        # Column names for the playlisters table
        playlister_columns = [desc[0] for desc in cur.description]

        # Create a dictionary for the playlister data
        playlister_data = dict(zip(playlister_columns, playlister_row))

        cur.execute("""
            SELECT *
            FROM playlists
            WHERE playlisterid = %s
        """, (playlister_data["playlisterid"],))

        playlists_rows = cur.fetchall()
        # Column names for the playlists table
        playlist_columns = [desc[0] for desc in cur.description]

        # Create a list of dictionaries for the playlists data
        playlists_data = [dict(zip(playlist_columns, row)) for row in playlists_rows]

        cur.execute("""
            SELECT *
            FROM playlistcampaigns
            WHERE playlisterid = %s
        """, (playlister_data["playlisterid"],))

        campaign_rows = cur.fetchall()
        campaign_columns = [desc[0] for desc in cur.description]
        campaigns_data = [dict(zip(campaign_columns, row)) for row in campaign_rows]

        return playlister_data, playlists_data, campaigns_data

    except Exception as e:
        pyperclip.copy(f"FAIL: {e}")
        return None, None, None
    finally:
        cur.close()

def update_playlister(spotifyuserid, conn, updated_playlister_data):
    cur = conn.cursor()
    print("beginning update on playlister")

    if not updated_playlister_data:
        print("No playlister data to update")
        return

    set_str = ', '.join([f"{key} = %s" for key in updated_playlister_data.keys()])
    values = list(updated_playlister_data.values()) + [spotifyuserid]
    sql = f"UPDATE playlisters SET {set_str} WHERE spotifyuserid = %s RETURNING playlisterid"

    try:
        cur.execute(sql, values)
        conn.commit()
        print("Playlister Updated")
    except Exception as e:
        conn.rollback()
        print(f"Error updating playlister: {e}")
    finally:
        cur.close()

def update_or_insert_playlist(conn, playlisterid, playlist_data):
    cur = conn.cursor()
    print("beginning update or insert on playlist")
    playlistid = None
    playlist_data['lastedited'] = datetime.now()

    try:
        cur.execute("SELECT playlistid FROM playlists WHERE playlistspotifyid = %s AND playlisterid = %s",
                    (playlist_data['playlistspotifyid'], playlisterid))
        result = cur.fetchone()
        print("Matching playlist in DB:", result)

        if result:  # Update
            playlistid = result[0]
            set_str = ', '.join([f"{key} = %s" for key in playlist_data.keys()])
            values = list(playlist_data.values())
            sql = f"UPDATE playlists SET {set_str} WHERE playlistspotifyid = %s AND playlisterid = %s"
            cur.execute(sql, values + [playlist_data['playlistspotifyid'], playlisterid])
        else:  # Insert
            columns = ', '.join(['playlisterid'] + list(playlist_data.keys()))
            placeholders = ', '.join(['%s' for _ in ['playlisterid'] + list(playlist_data.values())])
            values = [playlisterid] + list(playlist_data.values())
            sql = f"INSERT INTO playlists ({columns}) VALUES ({placeholders}) RETURNING playlistid"
            cur.execute(sql, values)
            playlistid = cur.fetchone()[0]

        conn.commit()
        print('Playlist added or updated')
        return playlistid
    except Exception as e:
        conn.rollback()
        print(f"Error updating or inserting playlist: {e}")
        pyperclip.copy(f"FAIL: Error updating or inserting playlist: {e}")
    finally:
        cur.close()

def update_or_insert_playlistcampaigns(conn, playlisterid, playlistid, campaign_data):
    cur = conn.cursor()
    print("beginning update or insert on playlistcampaign")
    campaign_data['lastedited'] = datetime.now()

    try:
        cur.execute("""
            SELECT * FROM playlistcampaigns
            WHERE playlistid = %s AND campaignid = %s AND playlisterid = %s
        """, (playlistid, campaign_data['campaignid'], playlisterid))

        result = cur.fetchone()

        if result:  # Update
            set_str = ', '.join([f"{key} = %s" for key in campaign_data.keys()])
            values = list(campaign_data.values()) + [playlistid, campaign_data['campaignid'], playlisterid]
            sql = f"UPDATE playlistcampaigns SET {set_str} WHERE playlistid = %s AND campaignid = %s AND playlisterid = %s"
            cur.execute(sql, values)
        else:  # Insert
            # Ensure playlisterid and playlistid are in the columns to insert
            campaign_data['playlisterid'] = playlisterid
            campaign_data['playlistid'] = playlistid

            columns = ', '.join(campaign_data.keys())
            placeholders = ', '.join(['%s' for _ in campaign_data.values()])
            values = list(campaign_data.values())
            sql = f"INSERT INTO playlistcampaigns ({columns}) VALUES ({placeholders})"
            cur.execute(sql, values)

        conn.commit()
        print('PlaylistCampaigns Updated')
    except Exception as e:
        conn.rollback()
        print(f"Error updating or inserting playlist campaigns: {e}")
        pyperclip.copy(f"FAIL: Error updating or inserting playlist campaigns:  {e}")
    finally:
        cur.close()

def handle_differences_and_update(conn):

    args = parse_arguments()

    playlister_data_new, playlist_data_new, campaign_data_new = organize_args_data(args)

    # Retrieve existing playlister details
    playlister_data_old, playlists_data_old, campaigns_data_old = get_playlister_details(args.user_id, conn)

    # Compare playlister data

    updated_playlister_data = compare_data(playlister_data_old, playlister_data_new)
    updated_playlister_data["spotifyuserid"] = playlister_data_old.get("spotifyuserid")

    # Compare multiple playlists
    updated_playlists_data = []
    handled_playlist_ids = set()

    # Update existing playlists
    for old_data in playlists_data_old:
        playlist_id = old_data.get("playlistspotifyid")

        if playlist_data_new.get("playlistspotifyid") == playlist_id:
            diffs = compare_data(old_data, playlist_data_new)
            diffs["playlistid"] = old_data.get("playlistid")
            diffs["playlistspotifyid"] = old_data.get("playlistspotifyid")

            if diffs:
                updated_playlists_data.append(diffs)

            handled_playlist_ids.add(playlist_id)

    # Add new playlists if the new playlist isn't in the handled list
    if playlist_data_new.get("playlistspotifyid") not in handled_playlist_ids:
        updated_playlists_data.append(playlist_data_new)

    print(f"Updated playlists data to handle: {updated_playlists_data}")


    updated_campaigns_data = []
    is_existing_campaign = False  # Flag to check if it's an existing campaign

    for old_data in campaigns_data_old:
        campaign_id = old_data.get("campaignid")

        if campaign_data_new.get("campaignid") == campaign_id:
            is_existing_campaign = True
            diffs = compare_data(old_data, campaign_data_new, force_include_keys=["campaignid"])
            if diffs:
                diffs["playlistid"] = old_data.get("playlistid")
                updated_campaigns_data.append(diffs)
                break  # Exit loop since we've found a match and updated

    # If after iterating over all old data, no match is found, it's a new campaign.
    if not is_existing_campaign:
        # Simply append the new campaign data to updated_campaigns_data for insertion later
        updated_campaigns_data.append(campaign_data_new)


    # Decide on updates based on differences
    if updated_playlister_data or updated_playlists_data or updated_campaigns_data:
        first_playlist_data = updated_playlists_data[0] if updated_playlists_data else None
        first_campaign_data = updated_campaigns_data[0] if updated_campaigns_data else None

        print('new playlister data =>', updated_playlister_data)
        print('new playlist data =>', first_playlist_data)
        print('new campaign data =>', first_campaign_data)

        handle_updates(conn, args.user_id, updated_playlister_data, first_playlist_data, first_campaign_data)

def handle_updates(conn, spotifyuserid, playlister_data, playlist_data, campaign_data):
    # Only update Playlister if there's actual change in the data
    playlistid = playlist_data.get('playlistid')
    if playlister_data and len(playlister_data) > 1:  # More than just spotifyuserid
        update_playlister(spotifyuserid, conn, playlister_data)

    # Retrieve current Playlister details
    current_playlister_data, current_playlist_data, current_campaign_data = get_playlister_details(spotifyuserid, conn)


    # If Playlister exists
    if current_playlister_data:
        playlisterid = current_playlister_data['playlisterid']

        try:
            # Check if playlist_data is available and has more than just 'playlistspotifyid' and db ID

            if playlist_data and len(playlist_data.keys()) >= 2:
                print(f"Attempting to update or insert playlist with data: {playlist_data}")

                # Use the update_or_insert_playlist function directly
                playlistid = update_or_insert_playlist(conn, playlisterid, playlist_data)

            # Update or Insert PlaylistCampaigns, if we have a valid playlistid

            if campaign_data and playlistid and len(campaign_data.keys()) >= 2:
                print(f"Attempting to update or insert campaign with data: {campaign_data}")
                update_or_insert_playlistcampaigns(conn, playlisterid, playlistid, campaign_data)
            success_message = f"Update successful.\nPlaylist ID: {playlistid}\nPlaylister ID: {playlisterid}"
            pyperclip.copy(success_message)
            print(success_message)
        except Exception as e:
            pyperclip.copy(f"FAIL: {e}")
            print(f"FAIL: {e}")

if __name__ == "__main__":
    # Check and install dependencies
    dependencies = ["subprocess", "psycopg2", "pyperclip", "argparse"]
    for package in dependencies:
        check_and_install(package, "pip3")

    # Set up the connection pool
    try:
        db_pool = setup_connection_pool_with_retry()
    except Exception as e:
        pyperclip.copy(f"FAIL: {e}")
        print("Connection pool error")
        sys.exit(1)
    print("Connection pool set up successfully!")


    conn = db_pool.getconn()

    handle_differences_and_update(conn)

    db_pool.putconn(conn)
