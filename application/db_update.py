import db_connect


if __name__ == '__main__':

    data = db_connect.Twitch.run_fields()

    for index, row in enumerate(data):
        db_connect.update_table(row)
        print(index)


