# Watched Event Producer 

## Program Description

Simulates the processing of Video Stream Events, waiting for START and STOP events for the same session to be received, before publishing a Content Watched Events, recording how long a user watched content for.

The program is parameterised to ensure that content must be watched for a minimum amount of time and any STOP events received 24 hours after the START event are ignored.


## Design assumptions

- Only two events a START and a STOP for the same session can be published; if the job is being restarted then duplicates can be rejected e.g. if two START events were received in succession.
- There was a choice between using Fixed Windows or Session Windows. I chose the latter since a minimal amount of events per session key will be received. - Since there are only two events per session, there will be no hot keys utilising resources
- There could possibly be knowledge per category of content on what the best session window gap duration should be, rather than using a general duration across all content.


## Design Approach

There are two jobs:

- A job that reads events from a file and publishes them to a pubsub video stream events topic
- A Job that reads video stream events from a pubsub topic and publishes content watched events to another sink, the content watched events topic
- To get the Session winding working correctly I used a "timestamp" attribute so that pubsub could use the event's timestamp in the VideoStreamEvent POJO as the event time injected into the pipeline
- The session windows are keyed on sessionId


## Testing Approach

- I used a file containing CSV style entries, one video stream event per line, to drive the creation of the video stream events
- Each event is submitted to pubsub in the order the events are listed in the file
- When the video stream events are created, there is a duration associated with each event; this allows the control of event time for each START / STOP event from a starting point, which depending on the session window gap duration, can determine how many windows are emitted and how many events there are per window. 
- There is one main JUnit test that qualifies that the session window is working correctly. Since the source was file based, the event timestamp had to be added to each event prior to any processing by the windowing components.

The video stream events file has a STAT & STOP event for three sessions.

The Test conditions were as follows:

- 70sec Gap duration
- min content duration of 70sec
- max content duration of 100sec

There were three sessions:

- session123, two events one at T0, one at T0 + 60secs: this would emit 1 window with two events (start & stop). This would not generate a content watch event, since the duration (60sec) was less than 70secs;
- session124, two events one at T0 + 30secs, one at T0 + 120secs: this would emit 2 windows with one event in each. This would generate a content watch event, since the duration (90sec) was greater than 70secs;
- session125, two events one at T0 + 10secs, one at T0 + 120secs: this would emit 2 windows with one event in each. This would not generate a content watch event, since the duration (110sec) was greater than 70secs;



## Using the Pubsub Emulator

This was a useful tool that allowed the testing of pubsub based pipelines locally using a local fake pubsub host/server.

There was a major problem with using the DirectRunner and the pubsub emulator, that being that the event timestamp wasn't injected into the events injected into the pipeline. This seriously impacted the session windowing!!! i.e. it didn't work.

Running the same pipelines on Dataflow in the cloud proved that the pipelines were running successfully, with the event timestamp present.



## How to the run the program

### Install Maven

Maven will be required to build and run the program. You can find instructions on how to download and install maven here: 

```
https://www.apache.org/
```


### Download the project using git

Download the project using git and then run the following git command:
```
git clone https://github.com/alexander-j-gardner/sky-beam-exam.git
```


### Build the project

Build the project using Maven to compile the code.

```
mvn clean install
```

There is currently no executable JAR; there are a number of java classes that need to be run to launch the BEAM jobs using Dataflow or Direct runners.


### Running the Video Stream Event Consumer

- When running in Direct mode using the local pubsub emulator the following program args are required:

```
--project=sky-project --textFilePath=/Users/alexandergardner/Documents/DataFlowPOC/SteveCode/event-load-job/src/resources/video-stream-events.txt --pubsubRootUrl=http://127.0.0.1:8085 --runner=DirectRunner --streaming=true
```


## Improvements

- I had difficulty getting either PubsubIO to serialize the VideoStreamEvent using AVRO
- I tried to use fasterxml's AvroMapper or simply used the AvroCoder but ultimately this led to various errors that I didn't solve (yet!). In previous projects I used custom Coders to serialise non-serialisable objects. 
- Would add more Units to prove that so 
