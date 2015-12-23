# twitter-lang
analysis of language use in tweets

# Building
You will need:
* sbt (1.0 or later)
* java (I use v8)

    sbt clean compile

# Running locally

All the following require a Twitter
* CONSUMER_KEY
* CONSUMER_SECRET
* ACCESS_TOKEN
* ACCESS_TOKEN_SECRET

(You can get all of these by creating your own app at https://apps.twitter.com/)

## Direct sbt

    sbt "run-main com.houseofmoran.twitter.lang.ReadTweetsApp $CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET"

Go to http://localhost:4040/ and you will see the spark UI. You may have to cycle through 4040, 4041, 4042 etc if you have any other spark apps runnijng locally.

Note that nothing will be printed on console for 5 mins at least.

## Docker
(Work in progress)

You will need:
* docker (I used 1.9.1, but will likely work on anything that has docker-machine)

## Setup docker machine
Spark silently does misleadingly little if you don't give it > 1 core to use. By default, on mac os x, docker-machine uses virtualbox to create an vm with only one cpu allocated. We need more. The following creates one with 4 but >= 2 *should* be ok.

    docker-machine create --driver virtualbox --virtualbox-cpu-count "4" spark-machine

Get your docker commands using this by doing

    eval $(docker-machine env spark-machine)

## Build image

    docker build -t twitter-lang .

## Run

    docker run -p 4040:4040 -it twitter-lang $CONSUMER_KEY $CONSUMER_SECRET $ACCESS_TOKEN $ACCESS_TOKEN_SECRET

## Monitor

Run

    docker-machine env spark-machine

Note which IP is used in DOCKER_HOST, and go to http://$DOCKER_HOST:4040/ which should be the spark UI. As above, nothing will be printed until 5 mins or more.
