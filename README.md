# Twitch Ex Machina

Machine Learning for Twitch.tv Broadcasters! This web app constantly polls twitch for data and processes it for machine learning algorithms

## Getting Started

Twitch Ex Machina needs some api keys to run

**PostgreSQL** 
* DB_NAME='AwesomeDB'
* DB_USER='AwesomePerson'
* DB_PASS='hunter2'
* DB_HOST='google.com'
* DB_PORT='5432'

**Twitch API** 
* TWITCH_API={'client-id': 'hunter2'}

**Giantbomb API** 
* GIANTBOMB_NAME={'user-agent': 'AwesomeBot'}
* GIANTBOMB_API='hunter2' 

**Plotly API** 
* PLOTLY_NAME='AwesomePerson'
* PLOTLY_API='hunter2'
* STREAM_KEY=['hunter2', 'hunter2', 'hunter2', ...],


## Calling db updates

Call these functions with heroku scheduler or a worker

**Games Update**
* python application/db -g
* 10 minutes

**Features Update**
* python application/db -f
* hourly

**Streams Update** 
* python application/db -t
* hourly
* Lots of data here (200000+ streams)

## Calling machine model updates

**Time Models****
* python application/machine -p 
* daily

**Full model****
* python application/machine -f
* weekly

## Calling plot updates

**Summary Plots**
* python application/db -p
* weekly

**Time Model Plots**
* python application/machine -t 20
* weekly

