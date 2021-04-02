# This your assignment report

## Part 1 - Design for streaming analytics

### 1. As a tenant, select a dataset suitable for streaming data analytics as a running scenario. Explain the dataset and why the dataset is suitable for streaming data analytics in your scenario. As a tenant, present at least two different analytics in the interest of the tenant: (i) a streaming analytics (tenantstreamapp) which analyzes streaming data from the tenant and (ii) a batch analytics which analyzes historical results outputted by the streaming analytics. The explanation should be at a high level to allow us to understand the data and possible analytics so that, later on, you can implement and use them in answering other questions. (1 point)

Selected dataset: Yellow Taxi Trip Data

Yellow Taxi Trip data contains *pick-up* and *drop-off time/locations, trip distances, number of passengers, payment type* and *fare amount*. It is of great value for analysing information in many different dimensions such as trip peaks/valleys, trip trend and traffic conditions of different locations in different time.

This kind of data has great timeliness because it is fast-changing, which requires real-time processing. Thus it is suitable for streaming data analytics.

As a tenant, I would like to do following analytics:

(i) streaming analytics: analyzing "estimated time" and "estimated fare" for every on-going trip in real-time.

(i) batch analytics: analyzing "trip peaks and valleys of a day", "average trip distance", "average fare of day/month/season and trend", "average tips" and "passenger preferenecs of payment types".

### 2. The tenant will send data through a messaging system which provides stream data sources. Discuss and explain the following aspects for the streaming analytics: (i) should the analytics handle keyed or non-keyed data streams for the tenant data, and (ii) which types of delivery guarantees should be suitable for the stream analytics and why. (1 point)

(i) Yes, using keyed data streams is more reliable and flexible although it takes up more storage space. For my big data platform, the data streams need to be keyed for the purpose of **identifying tenants**, **prevention of duplicated data** and **data partitioning and aggregating**

(ii) Basically there are two types of delivery guarantees:

- **At-least-once delivery** (a message will not be lost, but it might be delivered more than once)
- **At-most-once delivery**  (a message will only be delivered once, but it might be lost)

In this assignment, RabbitMQ is used for messaging system. RabbitMQ offers delivery guarantees through many different ways:

- For strong at-least-once delivery guarantees: quorum queues/mirrored queues + publisher confirms with mandatory flag + manual consumer acknowledgements + persistent messages

- For strong at-most-once delivery guarantees: classic queues with durable flag + no publisher confirms + auto consumer acknowledgements + no persistent messages

For stream analytics, a strong at-least-once delivery guarantee is obviously unsuitable because of the need of real-time; A strong at-most-once delivery guarantee is also unsuitable because it sacrifices the data accuracy.

In my perspective, the delivery guarantee that achieves a balance of message safety and performance is suitable, for which I would use: classic queues with durable flag + publisher confirms + manual consumer acknowledgements with a high prefetch + persistent messages
