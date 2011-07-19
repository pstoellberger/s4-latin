S4 Latin
==============================
A Streaming Language for the Distributed Stream Computing Platform S4
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



Running the Sample
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

# deploy the sample application into the S4 image (relies in the S4_IMAGE environmental variable)
cp -fpr target/dist/ $S4_IMAGE/s4-apps/myapp/

# Start server with s4-latin app and afterwards the client adapter as well
sh start_app.sh

# Check output
cat /tmp/speech.out.csv
or
cat /tmp/speech.out.json

</pre>

Samples
---------------------------------------

Introduction
---------------------------------------
All samples are included in the directory files/ in the applications directory.
Please note that most files are referenced using res: to avoid path issues.
If you change the s4latin files in the directory "files" it won't have any effect, since the VfsFileReader will read the versions in the packaged JAR file.
Those samples are just for demonstration purposes. You need to use absolute paths if you want to use your own examples!

In order to enable the sample you want (sample 1 is default) you need to edit the "adapter-conf.xml" file in the application's directory
and change the following line accordingly (sample.s4latin => sample2.s4latin or sample3.s4latin):

        <value>res:sample.s4latin</value>

You have to do the same for the cluster components.
For each sample there is a separate file. The one that will be used by default is always called "s4-latin-conf.xml".
So if you want to switch to another sample, rename the according file: sample2.s4-latin-conf.xml => s4-latin-conf.xml


<b> Sample 1 : File extraction, selection and projection, persist to file (sample.s4latin) </b>
<pre>

// The VfsFileReader can process files of type CSV, JSON or TEXT (TEXT will result in 1 column called "line")
create stream input as Source(io.s4.latin.adapter.VfsFileReader,file=res:speech.in;type=JSON)

filtered = select id,time,speaker from input where "speaker" = 'franklin delano roosevelt' or "speaker" = 'richard m nixon'

persist stream filtered to Output(io.s4.latin.persister.FilePersister,type=CSV;file=/tmp/speech.out.csv;delimiter=\t)
persist stream filtered to Output(io.s4.latin.persister.FilePersister,type=JSON;file=/tmp/speech.out.json;)

</pre>

<b> Sample 2 :Twitter-Feed Reader (sample2.s4latin) </b>
<pre>


// make sure you replace xxxx with your username and password
create stream input as Source(io.s4.latin.adapter.TwitterFeedListener,user=xxxx;password=xxxx;url=http://stream.twitter.com:80/1/statuses/sample.json)

Twitter = select id,created_at,text from input where "truncated" = 'true'
persist stream Twitter to Output(io.s4.latin.persister.FilePersister,type=CSV;file=/tmp/truncated_twitter_data;delimiter=\t\t)
</pre>

<b> Sample 3 : Apache access.log Parser (combination of manually configured PEs and s4latin (sample3.s4latin) </b>
<pre>
// use the accesslog parser PE in the s4-latin-conf.xml in combination with the s4latin process defined below

create stream RawLog as Source(io.s4.latin.adapter.VfsFileReader,file=res:mini-access.log;type=TEXT)
bigrows = select request,date,bytes from AccesslogRow where "bytes" > '20000' and "response" = '200'
persist stream bigrows to Output(io.s4.latin.persister.FilePersister,type=JSON;file=/tmp/bigrows;)
</pre>

In combination with PEs defined in the s4-latin-conf.xml
<pre>
'''
 <bean id="accesslogParserPE" class="io.s4.examples.logstats.pe.AccessLogParserPE">
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
        <value>res:sample3.s4latin</value>
      </list>
    </property>
    <property name="processPEs" value="true"/>
  </bean>
'''  
</pre>


Developing with Eclipse
-----------------------

The project contains already all metadata files to import it as Eclipse project.
You can always refresh this by using the maven eclipse plugin:

mvn eclipse:clean eclipse:eclipse

