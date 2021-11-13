# CS3103 Assignment 2: Job Scheduler

## Overview

The main idea of the algorithm is to assign a priority value for each server. The priority will act as a score for the desirability of the server at that point in time.

In most cases, when a new file arrives, we will assign the file to the server which has the highest priority. The priority value is calculated based on a basket of attributes, e.g. the estimated bandwidth. More details of the priority value will be given in a subsequent section.

Furthermore, we also rely on special rules to select the server, which overrides the priority. More details of these rules will be given in a subsequent section.


## Priority (P)

The priority is used as a proxy for the efficiency of the server at a momment in time. It is a single value, which is calculated based on a set of attributes. More specifically, it consists of the server's bandwidth (B), proportion of finished load (PFL), and active load (AL).

The relationship is illustrated in the following equation:

```
P = B * PFL / AL
```

The first component of P consists of the server's bandwidth (or efficiency to put it more generally). We calculate the bandwidth of a server by recording the arrival and departure time of files with known file sizes. Then the bandwidth can be calculated by dividing the file size by the time interval. A server which has a higher bandwidth will then be given a higher P.

However, this would not be impossible if there are no files with known sizes. Therefore, we made use of proportion of finished load.  PFL is cacluated by dividing the size of all completed files by the server by the size of all completed files by all servers. This gives a relative score of each server's efficiency and is extremely informative when there are no files with known sizes. A server which has completed more workload will tend to be a more efficient server. Hence, it will have a  higher P.

Second, we used the active load of the server, which is calculated by taking the sum of the size of all files that are currently being processed by the server. A server with a large amount of active workload
will have a higher AL and thus, a lower P.


## Rules

One deficiency of Priority is that it may be a poor score in the initial phase and so it may not provide an accurate indication of the server's efficiency. Therefore 2 special rules are implemented to help alleviate some of the issues in the initial phase: Force feeding and evolving priority.

### Force feeding

In cases where file sizes are available, the bandwidth is a crucial quantity that will give us a reliable comparison of the servers' efficiency. Therefore, having this information as soon as possible will be beneficial. This requires us to give the target server a file will known file size. Once the results of this file returns, we would be able to calculate the bandwidth. The main ideal in force feeding is to send a file with known size to a server that has not received a file with known size before, regardless of the current workload of this server. This ensures that all the servers will get to process a file with known size as earlier as possible, thereby allowing us to calculate its bandwidth and update its priority accordingly.

### Evolving priority

In the composition of our priority, we included PFL so that it can be used as a proxy for the server bandwidth when there are no files with known sizes. However, in the initial phase, PFL may not be a reliable indicate because some servers might have received the files much earlier than others, allowing them to accumulate more finished files.

Therefore, to prevent the biase in the initial phase, we simply the priority to:

```
P = B / AL
```

omitting the PFL. As time progresses, we will incorporate PFL to enhance our understanding the server's efficiency.
