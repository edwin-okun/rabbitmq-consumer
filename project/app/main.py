import pika, sys, os
from datetime import datetime


# Define connection parameters once
params = pika.ConnectionParameters(
    host='localhost',
    heartbeat=600,
    blocked_connection_timeout=300
)


def main():
    # Create a new connection
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # make sure that the queue exists
    channel.queue_declare(queue='hello')

    # callback fxn to subscribe to a queue
    def callback(ch, method, properties, body):
        print(f" [x] Received {body.decode('utf-8')} at {datetime.now()}")

    # consume queue contents
    channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)