import praw
import json
import time

reddit = praw.Reddit(
    client_id = "nBROxkqXX7OBrfnBFPxLLw",
    client_secret = "drO4H7CGeSg7XqTrfrTseCeqgAmGMA",
    user_agent = "testrun1",
    username = "cs651"
)

def fetch_and_send(subreddit_name, conn):
    subreddit = reddit.subreddit(subreddit_name)
    for submission in subreddit.search("tariff", limit=100):
        data = {
            "title": submission.title,
            "text": submission.selftext[:500],
            "url": submission.url,
            "score": submission.score
        }
        message = json.dumps(data) + "\n"
        conn.sendall(message.encode("utf-8"))
        time.sleep(0.1)

if __name__ == "__main__":
    host = "localhost"
    port = 9999

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Listening on {host}:{port}...")

    conn, addr = server_socket.accept()
    print(f"Client connected from {addr}")

    fetch_and_send("investing", conn)

    conn.close()
    server_socket.close()
