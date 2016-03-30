import db_connect


def main():

    twitch = db_connect.Twitch()
    giantbomb = db_connect.Giantbomb()

    conn = db_connect.connect()
    cur = conn.cursor()

    trialid = db_connect.get_trialid(cur)

    for index, row in enumerate(twitch.fields):
        db_connect.update_table(row, trialid, giantbomb, cur)
        print(index)

    conn.commit()
    conn.close()

main()

