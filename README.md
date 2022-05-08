# Oracle XML Log File Merger

## Introduction

This small Java program was made to merge the XML log provided by Oracle from different nodes in a cluster:

In a RAC cluster or having a Data Guard setup you find several ASM and RDBMS instances.
Normally in a two node RAC you have two ASM instances and two RDBMS instances.

Beginning with Oracle Release 11g, the alert log is written as both an XML-formatted file and as a text file.
Both formats do have pros and cons:

- The text file is not managed by the automatic diagnostic repository routines. Hence it never loses information and only grows in size if you never rotate or cut it.
- On the other hand it is hard to parse by scripts as every entry only contains a timestamp, followed by newline and the entry.
- Several entries might be grouped together and only show one timestamp - for example if you start the instance.
- The XML file in contrast is capped in size and numbers. So it might lose information.
- Every entry is logged with a timestamp and holds only one message. But several messages might be grouped together, which is marked in the XML attributes.
- But it can be easily parsed by scripts and programs due to its XML based structure. (Except for the fact that the log is not written in a valid XML format as a single root element is missing.)

In case of a failure, a disaster, problem etc. you might need to ask

> "This happened on this instance - but what happened at the same time on the other nodes?".

Normally you open several windows, hold them side by side and compare the entries.

## Description

This Java program is used to merge given XML logs together.
As the logs might be very large it parses it using a SAX parser.
Every entry in a alert log is made of

```xml
<msg time='Timestamp With Timezone offset' org_id='oracle' comp_id='component'
 <!-- other attributes --> >
 <txt>message text
 </txt>
</msg>
```

The attributes you might see:

- **comp_id** is the software component.
    - For ASM it is **asm**,
    - for the DB it is **rdbms**,
    - **kfed** for example uses **asmtool**,
    - the listeners use **tnslsnr** etc.
- **host_id** shows the hostname.
- **pid** shows the process id of the process sending the message.
- **group** shows that several messages were sent together.

By theory OLM should be able to import the XML log of all components provided by Oracle Corp.
That way one can see the messages of ASM, RDBMS, Listener and CRS in one file (that would be large for sure).

## Merging of XML files

To have several XML files (that are possibly large in size) merged together, this Java program uses a H2 database to store all messages in a message table.
That means - at the moment - that the program must be called once for every XML logfile.
After all logs are processed the program can be used to dump all messages sorted by date into a CSV file.

## Installation and usage

For my tests I used the LTS JSE 17 using the OpenJ runtime environment provided by [IBM](https://developer.ibm.com/languages/java/semeru-runtimes/).
I'm not using gradle, ant or similar but just used the following commands you can follow as well:

- Download and extract [Apache Commons CLI](https://commons.apache.org/proper/commons-cli/) (I used version 1.5.0.) into `lib`.
- Download and extract [Apache Commons CSV](https://commons.apache.org/proper/commons-csv/) (I used version 1.9.0.) into `lib`.
- Download and extract [H2 Database](https://h2database.com/html/main.html) (I used Version 2.1.212) into `lib`.
- Clone the [source repo](https://github.com/jhorchler/oracle-log-merger.git) or just download [Olm.java](https://raw.githubusercontent.com/jhorchler/oracle-log-merger/trunk/Olm.java).
- As this version of Java is able to execute a java file on the fly, not compilation is needed.

### Import XML logfiles

A single XML file can be imported using:

```powershell
java --class-path ".\lib" --module-path ".\lib" --add-modules ALL-MODULE-PATH -D"jdbc.drivers"="org.h2.Driver" Olm.java -db ${directory_and_file_for_h2_db} -l ${directory_and_file_for_your_xml_log}
```

Several files can be imported using a shell loop.
For example in Powershell:

```powershell
cd "C:\XMLLogs"
foreach ($filename in (Get-ChildItem|Select-Object FullName)) { java --class-path ".\lib" --module-path ".\lib" --add-modules ALL-MODULE-PATH -D"jdbc.drivers"="org.h2.Driver" Olm.java -db ${directory_and_file_for_h2_db} -l $filename.FullName }
```

### Dump all log entries into a single CSV file using

To dump all entries of the message table just use:

```powershell
java --class-path ".\lib" --module-path ".\lib" --add-modules ALL-MODULE-PATH -D"jdbc.drivers"="org.h2.Driver" Olm.java -db ${directory_and_file_for_h2_db} -p C:\oxlm\asm_alerts.csv
```

## Further usage

The H2 database created in that way might be used for example to create a full text search index available.
H2 provides a shell for that. Please visit the [website](https://h2database.com/html/main.html) for that.

In the future I might extent this class for example

- to walk a directory tree and import all XML files found, or
- to import and dump in one step using a memory H2 database that is lost immediately.
