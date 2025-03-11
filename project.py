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

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 project.py import <folderName>")
        return

    command = sys.argv[1]
    folderName = sys.argv[2]

    if command == "import":
        import_data(folderName)
    else:
        print("Unknown command")

main()