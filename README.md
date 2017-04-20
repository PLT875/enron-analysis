# Enron Analysis

The application performs analysis on the Enron email data set to find the top email recipients and average word count of the messages.

## Prerequisites

Minimum requirements are:
* Scala (2.11.8)
* Java 8
* SBT (0.13.11)
* Spark (2.0.0, included in the SBT dependencies and runs in local mode only)


## Getting started

Fetch dependencies
```
sbt update
```

Run unit tests
```
sbt test
```

Run application
```
sbt "run <enron_directory>"
```

Where <enron_directory> is the absolute path of all the Zip files to read e.g. /home/ec2-user/enron/edrm-enron-v2

## Assumptions / design considerations

* The edrm-enron-v2 version of data set is used
* Zip files ending with _xml.zip are read
* The XML files of the ZIP are analyzed, documents with a DocType of "message" are used to extract the primary and carbon copy recipients, and message lines (from the corresponding external text file)
* For simplicity only valid email addresses are extracted and read
* Words are defined as white spaces between characters

## Results

The analysis was performed on a t2.medium instance and the Enron data volume was mounted.

Link to results output [here](results.txt). 

