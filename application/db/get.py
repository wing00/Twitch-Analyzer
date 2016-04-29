from application import app
from server import connect, HTTP_RE
import requests
import update


def trialid():
    """ fetches highest trial id number

    :return: (int) trial id
    """

    conn = connect()
    cur = conn.cursor()
    cur.execute('''SELECT trialid FROM snapshot ORDER BY trialid DESC LIMIT 1''')
    trialid = cur.fetchone()

    if trialid:
        trialid = trialid[0]
    else:
        trialid = 1
    conn.close()
    return trialid


def team_row(url, channel_id):
    """ accesses team api link of channel and updates the table row with the information

    :param url: api link of channel's team
    :param channel_id: channel id number
    :return: number of teams
    """
    team_url = HTTP_RE.sub(r'https://', url)
    teams = requests.get(team_url,
                         params=dict(limit=100),
                         headers=app.config['TWITCH_API']
                         ).json()

    if teams.get('teams', 0) != 0:
        for team in teams['teams']:
            team_row = dict(
                channel_id=channel_id,
                team_id=team['_id'],
                team_name=team['display_name']
                )
            update.team_table(team_row)
        return len(teams['teams'])

    return 0


def stream_row(offset):
    """extracts information from json into a dict

    :param field: json object for channel information
    :return: dict of relevant information from json
    """

    datas = requests.get('https://api.twitch.tv/kraken/streams',
                 params=dict(
                     limit=100,
                     offset=offset
                 ),
                 headers=app.config['TWITCH_API']
                 ).json()['streams']

    for field in datas:
        channel = field['channel']
        row = dict(
            sponsored=False,
            scheduled=False,
            featured=False,

            game=field['game'],
            viewers=field['viewers'],
            stream_id=field['_id'],

            mature=channel['mature'],
            language=channel['broadcaster_language'],
            channel_id=channel['_id'],
            partner=channel['partner'],
            url=channel['url'],
            total_views=channel['views'],
            followers=channel['followers']
        )

        team_count = team_row(channel['_links']['teams'], channel['_id'])
        video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
        videos = requests.get(video_url, headers=app.config['TWITCH_API']).json()
        video_count = videos['_total'] if videos['_total'] else 0

        row.update(dict(
            video_count=video_count,
            team_count=team_count
            )
        )
        update.stream_table(row)


def featured_row(field):
    """extracts information from json into a dict for featured streams
       extra level of information needs processing

    :param field: json object for channel information
    :return: dict of relevant information from json
        """
    stream = field['stream']
    channel = stream['channel']

    row = dict(
        sponsored=field['sponsored'],
        scheduled=field['scheduled'],
        featured=True,

        game=stream['game'],
        viewers=stream['viewers'],
        stream_id=stream['_id'],

        mature=channel['mature'],
        language=channel['broadcaster_language'],
        channel_id=channel['_id'],
        partner=channel['partner'],
        url=channel['url'],
        total_views=channel['views'],
        followers=channel['followers']
        )

    team_count = team_row(channel['_links']['teams'], channel['_id'])

    video_url = HTTP_RE.sub(r'https://', channel['_links']['videos'])
    videos = requests.get(video_url,
                          headers=app.config['TWITCH_API']
                          ).json()
    video_count = videos['_total'] if videos['_total'] else 0

    row.update(dict(
        video_count=video_count,
        team_count=team_count
        )
    )

    return row


def video_row(params):
    """extracts information from json into a dict

    :param field: json object for channel information
    :return: dict of relevant information from json
    """
    video, channel_id = params
    video_row = dict(
        channel_id=channel_id,
        video_id=video['_id'],
        video_type=video['broadcast_type'],
        video_title=video['title'],
        video_game=video['game'],
        video_desc=video['description'],
        video_status=video['status'],
        video_views=video['views'],
        video_url=video['url'],
        video_res=video['resolutions'],
        video_length=video['length']
        )
    return video_row
