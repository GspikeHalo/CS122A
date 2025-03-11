import sys
import os
import csv
import mysql.connector

def connect_db():
    return mysql.connector.connect(
        user='test',
        password='password',
        database='cs122a'
    )

def import_data(folderName):
    try:
        conn = connect_db()
        cursor = conn.cursor()

        mapping = {
            "Users": (
                "users.csv",
                "INSERT INTO Users (uid, email, joined_date, nickname, street, city, state, zip, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            ),
            "Producers": (
                "producers.csv",
                "INSERT INTO Producers (uid, bio, company) VALUES (%s, %s, %s)"
            ),
            "Viewers": (
                "viewers.csv",
                "INSERT INTO Viewers (uid, subscription, first_name, last_name) VALUES (%s, %s, %s, %s)"
            ),
            "Releases": (
                "releases.csv",
                "INSERT INTO Releases (rid, producer_uid, title, genre, release_date) VALUES (%s, %s, %s, %s, %s)"
            ),
            "Movies": (
                "movies.csv",
                "INSERT INTO Movies (rid, website_url) VALUES (%s, %s)"
            ),
            "Series": (
                "series.csv",
                "INSERT INTO Series (rid, introduction) VALUES (%s, %s)"
            ),
            "Videos": (
                "videos.csv",
                "INSERT INTO Videos (rid, ep_num, title, length) VALUES (%s, %s, %s, %s)"
            ),
            "Sessions": (
                "sessions.csv",
                "INSERT INTO Sessions (sid, uid, rid, ep_num, initiate_at, leave_at, quality, device) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            ),
            "Reviews": (
                "reviews.csv",
                "INSERT INTO Reviews (rvid, uid, rid, rating, body, posted_at) VALUES (%s, %s, %s, %s, %s, %s)"
            )
        }

        for table, (csv_file, insert_query) in mapping.items():
            file_path = os.path.join(folderName, csv_file)

            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader)  # 跳过表头
                rows = [tuple(row) for row in reader]

                if rows:
                    cursor.executemany(insert_query, rows)
                    conn.commit()
                    print(f"Inserted {len(rows)} rows into {table}")
                else:
                    print(f"No data in {csv_file}")

        print("Success")
    except Exception as e:
        print("Fail", e)
        raise
    finally:
        cursor.close()
        conn.close()


def insertViewer(uid, email, nickname, street, city, state, zip_code, genres, joined_date, first, last, subscription):
    pass


def addGenre(uid, new_genre):
    pass


def deleteViewer(uid):
    pass


def insertMovie(rid, website_url):
    pass


def insertSession(sid, uid, rid, ep_num, initiate_at, leave_at, quality, device):
    pass


def updateRelease(rid, title):
    pass


def listReleases(uid):
    pass


def popularRelease(N):
    pass


def releaseTitle(sid):
    pass


def activeViewer(N, start_date, end_date):
    pass


def videosViewed(rid):
    pass


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 project.py <function name> [params...]")
        return

    func = sys.argv[1]
    args = sys.argv[2:]

    if func == "import":
        if len(args) != 1:
            print("Usage: python3 project.py import <folderName>")
            return
        import_data(args[0])
    elif func == "insertViewer":
        if len(args) != 12:
            print("Usage: python3 project.py insertViewer <uid> <email> <nickname> <street> <city> <state> <zip> <genres> <joined_date> <first> <last> <subscription>")
            return
        insertViewer(*args)
    elif func == "addGenre":
        if len(args) != 2:
            print("Usage: python3 project.py addGenre <uid> <genre>")
            return
        addGenre(*args)
    elif func == "deleteViewer":
        if len(args) != 1:
            print("Usage: python3 project.py deleteViewer <uid>")
            return
        deleteViewer(*args)
    elif func == "insertMovie":
        if len(args) != 2:
            print("Usage: python3 project.py insertMovie <rid> <website_url>")
            return
        insertMovie(*args)
    elif func == "insertSession":
        if len(args) != 8:
            print("Usage: python3 project.py insertSession <sid> <uid> <rid> <ep_num> <initiate_at> <leave_at> <quality> <device>")
            return
        insertSession(*args)
    elif func == "updateRelease":
        if len(args) != 2:
            print("Usage: python3 project.py updateRelease <rid> <title>")
            return
        updateRelease(*args)
    elif func == "listReleases":
        if len(args) != 1:
            print("Usage: python3 project.py listReleases <uid>")
            return
        listReleases(*args)
    elif func == "popularRelease":
        if len(args) != 1:
            print("Usage: python3 project.py popularRelease <N>")
            return
        popularRelease(*args)
    elif func == "releaseTitle":
        if len(args) != 1:
            print("Usage: python3 project.py releaseTitle <sid>")
            return
        releaseTitle(*args)
    elif func == "activeViewer":
        if len(args) != 3:
            print("Usage: python3 project.py activeViewer <N> <start_date> <end_date>")
            return
        activeViewer(*args)
    elif func == "videosViewed":
        if len(args) != 1:
            print("Usage: python3 project.py videosViewed <rid>")
            return
        videosViewed(*args)
    else:
        print("Unknown function")

if __name__ == "__main__":
    main()