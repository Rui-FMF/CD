import sys
import argparse
import middleware

# 3 Tests that could not be achieved with only the consumer and producer classes provided
# Follow exact instructions or else results will not be as expected, make sure a broker is active

print("Starting multiple Subscription Test\n")

print("Creating consumer first subscribed to /temp")
consumer = middleware.JSONQueue("/temp")

print("Start a producer of topic /temp, so consumer can do 10 pulls")
for _ in range(10):
    topic, data = consumer.pull()
    print(topic, data)

input("Start a producer of topic /msg, so there is initial data consumer can receive at subscription\nPress enter when done\n")

print("Subscribing producer to /msg ...")
consumer.sub("/msg")

print("Start producers of topics /temp and /msg, consumer should receive data from both (20 pulls)")
for _ in range(20):
    topic, data = consumer.pull()
    print(topic, data)

print("Multiple Subscription Test ended\n\n")

print("Starting Unsubscription Test\n")
print("Unsubscribing producer from /temp ...")
consumer.unsub("/temp")

print("Start producers of topics /temp and /msg, consumer should only receive data from msg (20 pulls)")
for _ in range(20):
    topic, data = consumer.pull()
    print(topic, data)

print("Unsubscription Test ended\n\n")

print("Starting Topic Listing Test\n")

print("Using the listing method should show the topics /temp and /msg:")

consumer.list_topics()

input("Start a producer of topic /weather, so the topic gets registered in broker\nPress enter when done\n")

print("Using the listing method should now also show /wheater child topics:")

consumer.list_topics()

print("Topic Listing Test ended\n\n")