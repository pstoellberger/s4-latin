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
Sample 1 : File extraction, selection and projection, persist to file
<pre>

// INPUT
// The VfsFileReader can process files of type CSV, JSON or TEXT (TEXT will result in 1 column called "line")
create stream input as Source(io.s4.latin.adapter.VfsFileReader,file=res:speech.in;type=JSON)

// SELECTION / PROJECTION
// select only specific speakers

debugprojection = select id,time,speaker from input where "speaker" = 'franklin delano roosevelt' or "speaker" = 'richard m nixon'

// OUTPUT
// Persist the stream
persist stream debugprojection to Output(io.s4.latin.persister.FilePersister,type=CSV;file=/tmp/speech.out.csv;delimiter=\t)
persist stream debugprojection to Output(io.s4.latin.persister.FilePersister,type=JSON;file=/tmp/speech.out.json;)


</pre>


Developing with Eclipse
-----------------------

The project contains already all metadata files to import it as Eclipse project.
You can always refresh this by using the maven eclipse plugin:

mvn eclipse:clean eclipse:eclipse

