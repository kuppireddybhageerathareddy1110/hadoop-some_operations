import random
import time
import socket

def generate_stream_data():
    words = ["hadoop", "spark", "streaming", "bigdata", "analytics",
             "machine", "learning", "python", "java", "scala"]
    while True:
        sentence = ' '.join(random.choices(words, k=random.randint(3, 10)))
        send_to_flume(sentence)
        time.sleep(0.1)  # 10 messages/sec

def send_to_flume(message):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(('localhost', 44444))
        s.send((message + '\n').encode())
        s.close()
    except Exception as e:
        print(f"Error sending data: {e}")

if __name__ == "__main__":
    generate_stream_data()
