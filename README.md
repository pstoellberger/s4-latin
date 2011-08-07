S4 Latin
==============================
A Streaming Language for the Distributed Stream Computing Platform S4<br />
For more information, see [s4.io](http://s4.io)

Requirements
------------

* Linux
* Java 1.6

Build Instructions
------------------

* Build using Maven

	- USAGE: mvn TASK1, TASK2, ...
	
	- Main Tasks:
	
		+ clean: deletes all the build dirs
		+ package: creates a zip folder in target/ that contains the s4 application
		+ install: installs jars and POMs in local Maven repo (eg. ~/.m2)

* Build and deploy using Maven + shell script

    - You can build the application, deploy and run it in one go by executing
    $ sh run.sh
    
    You will need to have the environment variable S4_IMAGE set to a s4 image directory
    e.g: EXPORT S4_IMAGE=/opt/s4/s4image



Running the Examples
---------------------------------------
<pre>
#  Download the project files from the repository.
git clone git://github.com/pstoellberger/s4-latin.git

# set the S4_IMAGE environmental variable
export S4_IMAGE=<path to s4 image>
e.g:
export S4_IMAGE=/opt/s4/s4image

# build the package
mvn clean package

# deploy the example application into the S4 image (relies in the S4_IMAGE environmental variable)
cp -fpr target/dist/ $S4_IMAGE/s4-apps/myapp/

# Start server with s4-latin app and afterwards the client adapter as well
sh start_app.sh

# Check output
cat /tmp/speech.out.csv
or
cat /tmp/speech.out.json

</pre>

Examples
---------------------------------------

Introduction
---------------------------------------
All examples are included in the directory files/ in the applications directory.
Please note that most files are referenced using res: to avoid path issues.
If you change the s4latin files in the directory "files" it won't have any effect, since the VfsFileReader will read the versions in the packaged JAR file.
Those examples are just for demonstration purposes. You need to use absolute paths if you want to use your own examples!

In order to enable the example you want (example 1 is default) you need to edit the "adapter-conf.xml" file in the application's directory
and change the following line accordingly (example.s4latin => example2.s4latin or example3.s4latin):

        <value>res:example.s4latin</value>

You have to do the same for the cluster components.
For each example there is a separate file. The one that will be used by default is always called "s4-latin-conf.xml".
So if you want to switch to another example, rename the according file: example2.s4-latin-conf.xml => s4-latin-conf.xml


<b> Example 1 : File extraction, selection and projection, join with other stream and persist to file (example.s4latin) </b>

    
    // INPUT
    // The VfsFileReader can process files of type CSV, JSON or TEXT (TEXT will result in 1 column called "line")
    // create stream debuginput as Source(io.s4.latin.adapter.VfsFileReader,file=res:speech.in.csv;type=CSV;delimiter=\t;debug=true)

    create stream allspeeches as Source(io.s4.latin.adapter.VfsFileReader,file=res:speech.in;type=JSON)
    create stream sentences as Source(io.s4.latin.adapter.VfsFileReader,file=res:sentence.in;type=JSON)
    speech = select * from allspeeches where "speaker" = 'franklin delano roosevelt' or "speaker" = 'richard m nixon'
    joined = join(io.s4.latin.core.LatinJoinPE) on speech.id,sentences.speechId include speech.id,speech.location,speech.speaker,sentences.text window 100 seconds every 15 seconds
    persist stream joined to Output(io.s4.latin.persister.FilePersister,type=CSV;file=/tmp/joinedspeech;delimiter=\t)




<b> Example 2 :Twitter-Feed Reader (example2.s4latin) </b>

    // make sure you replace xxxx with your username and password
    create stream input as Source(io.s4.latin.adapter.TwitterFeedListener,user=xxxx;password=xxxx;url=http://stream.twitter.com:80/1/statuses/sample.json)
    
    Twitter = select id,created_at,text from input where "truncated" = 'true'
    persist stream Twitter to Output(io.s4.latin.persister.FilePersister,type=CSV;file=/tmp/truncated_twitter_data;delimiter=\t\t)


<b> Example 3 : Apache access.log parser (combination of manually configured PEs and s4latin (example3.s4latin + example3.s4-latin-conf.xml) </b>

    // use the accesslog parser PE in the s4-latin-conf.xml in combination with the s4latin process defined below
    create stream RawLog as Source(io.s4.latin.adapter.VfsFileReader,file=res:mini-access.log;type=TEXT)
    bigrows = select request,date,bytes from AccesslogRow where "bytes" > '20000' and "response" = '200'
    persist stream bigrows to Output(io.s4.latin.persister.FilePersister,type=JSON;file=/tmp/bigrows;)


In combination with PEs defined in the s4-latin-conf.xml
    
    <bean id="accesslogParserPE" class="io.s4.latin.core.AccessLogParserPE">
      <property name="id" value="accesslogPE"/>
      <property name="keys">
        <list>
          <value>RawLog *</value>
        </list>
      </property>
      <property name="columnName" value="line"/>
      <property name="dispatcher" ref="resourceDispatcher"/>
      <property name="outputStreamName" value="AccesslogRow"/>
    </bean>
  
    <bean id="resourceSeenPartitioner" class="io.s4.dispatcher.partitioner.DefaultPartitioner">
    <property name="streamNames">
      <list>
        <value>AccesslogRow</value>
      </list>
    </property>
    <property name="hashKey">
      <list>
        <value>key</value>
      </list>
    </property>
    <property name="hasher" ref="hasher"/>
    <property name="debug" value="false"/>
    </bean>

    <bean id="resourceDispatcher" class="io.s4.dispatcher.Dispatcher" init-method="init">
    <property name="partitioners">
      <list>
        <ref bean="resourceSeenPartitioner"/>
      </list>
    </property>
    <property name="eventEmitter" ref="commLayerEmitter"/>
    <property name="loggerName" value="s4"/>
    </bean>
  
  
    <bean id="latinModule" class="io.s4.latin.core.Module" init-method="init">
    <property name="latinFile" >
    <list>
        <value>res:example3.s4latin</value>
      </list>
    </property>
    <property name="processPEs" value="true"/>
    </bean>
    

<b> Example 4 : Apache access.log parser in pur s4latin. use UDF to parse </b>

    create stream RawLog as Source(io.s4.latin.adapter.VfsFileReader,file=res:mini-access.log;type=TEXT;columnName=line)
    arows = process stream RawLog with UDF(io.s4.latin.core.AccessLogParserPE,columnName=line;debug=true)
    bigrows = select request,date,bytes from arows where "bytes" > '20000' and "response" = '200'
    persist stream bigrows to Output(io.s4.latin.persister.FilePersister,type=JSON;file=/tmp/bigrows;)

<b> Example 5 : Stream Twitter feed and use Kettle UDF to generate top10 every minute </b>

    // adopted from the kettle streaming example : real-time streaming data aggregation with Kettle http://www.ibridge.be/?p=204
    create stream input as Source(io.s4.latin.adapter.TwitterFeedListener,user=xxxxxxx;password=xxxxx;url=http://stream.twitter.com:80/1/statuses/sample.json)
    tweets = select text,id,created_at,retweet_count,hashtag0,hashtag1,hashtag2,hashtag3,hashtag4 from input where "hashtag0" != ''
    tophashtags = process stream tweets with UDF(io.s4.latin.core.KettlePE,transformation=res:Read a twitter stream.ktr;input=injector;output=outputstep;loginterval=10)
    selectfields = select nr,hashtag,count,from,to from tophashtags
    persist stream selectfields to Output(io.s4.latin.persister.FilePersister,type=CSV;delimiter=\t;file=/tmp/toptags;)
 




Developing with Eclipse
-----------------------

The project contains already all metadata files to import it as Eclipse project.
You can always refresh this by using the maven eclipse plugin:

    mvn eclipse:clean eclipse:eclipse

Contact
-----------------------
Author:     Paul Stoellberger
Email:      pstoellberger@gmail.com


