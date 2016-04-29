from server import connect
from giantbomb import Giantbomb
import get


def table(row):
    """ checking and updating db with game info from giantbomb

    :param row: (dict) dict of twitch json object
    """

    conn = connect()
    cur = conn.cursor()

    trialid = get.trialid()
    Giantbomb.check(row['name'], row['giantbombid'])
    row['trialid'] = trialid + 1

    # adding values into snapshot table
    query = '''
              INSERT INTO snapshot VALUES (
                  DEFAULT,
                  %(name)s,
                  %(giantbombid)s,
                  %(trialid)s,
                  %(rank)s,
                  %(viewers)s,
                  %(channels)s,
                  current_timestamp
                )
            '''
    cur.execute(query, row)

    # updating game_name table: if no entry found add entry
    query = '''
                DO
                $do$
                BEGIN
                    IF EXISTS(SELECT * FROM game_name
                                  WHERE name = %(name)s)
                    THEN
                        UPDATE game_name
                            SET viewer_total = viewer_total + %(viewers)s,
                                channel_total = channel_total + %(channels)s,
                                rank_total = rank_total + %(rank)s,
                                trials = trials + 1
                            WHERE name = %(name)s;
                    ELSE
                        INSERT INTO game_name VALUES (
                            DEFAULT,
                            %(name)s,
                            %(giantbombid)s,
                            %(viewers)s,
                            %(channels)s,
                            %(rank)s,
                            1
                            );
                    END IF;
                END
                $do$
            '''

    cur.execute(query, row)
    conn.commit()
    conn.close()


def stream_table(row):
    """ Updates stream db
    :param row: row to insert to stream db
    """
    conn = connect()
    cur = conn.cursor()

    query = '''
        INSERT INTO stream VALUES (
            DEFAULT,
            %(stream_id)s,
            %(channel_id)s,
            %(url)s,
            %(language)s,
            %(scheduled)s,
            %(featured)s,
            %(mature)s,
            %(partner)s,
            %(sponsored)s,
            %(game)s,
            %(viewers)s,
            %(followers)s,
            %(total_views)s,
            %(video_count)s,
            %(team_count)s,
            current_timestamp
          )'''

    cur.execute(query, row)
    conn.commit()
    conn.close()


def team_table(row):
    """ Check if row exists. If not, insert to team table
    :param row: row to insert
    """
    if not row:
        return

    conn = connect()
    cur = conn.cursor()
    query = '''SELECT channelid, teamid FROM team
               WHERE channelid = %(channel_id)s
                  AND teamid = %(team_id)s'''

    cur.execute(query, row)
    fetch = cur.fetchone()

    if fetch is None:
        query = '''
            INSERT INTO team VALUES (
                %(channel_id)s,
                %(team_id)s,
                %(team_name)s,
                current_timestamp
              )'''

        cur.execute(query, row)
        conn.commit()

    conn.close()


def video_table(row):
    """ Inserts row to video table
    :param row: row to insert
    """
    if not row:
        return

    conn = connect()
    cur = conn.cursor()
    query = '''
            INSERT INTO featured VALUES (
                DEFAULT,
                 %(channel_id)s,
                 %(video_id)s,
                 %(video_title)s,
                 %(video_game)s,
                 %(video_status)s,
                 %(video_type)s,
                 %(video_views)s,
                 %(video_url)s,
                 %(video_res)s,
                 %(video_length)s,
                 %(video_desc)s,
                current_timestamp
              )'''
    cur.execute(query, row)
    cur.commit()
    conn.close()
