import db_connect

if __name__ == '__main__':

    stream_row, team_row = db_connect.Twitch.run_streams()

    for index, row in enumerate(stream_row):
        db_connect.update_stream_table(row)
        print index

    for index, row in enumerate(team_row):
        db_connect.update_team_table(row)
        print index

