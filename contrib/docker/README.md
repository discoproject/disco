Building
========
In order to build a docker disco image you need to
[install docker](https://www.docker.io/gettingstarted/). Then go to
`disco/contrib/docker` and build the disco image:

    $ docker build -t disco/disco .

This command will fetch Ubuntu images from a public docker index and
build an image called `disco/disco`.

Running
========
You can run the disco image as follows:

    $ docker run -p 8989:8989 -t -i disco/disco

Now disco is running inside the docker and you can see the web interface.

An SSH server is running on this image, and can be exposed by mapping it to
a host port:

    $ docker run -p 8989:8989 -p 2200:22 -t -i disco/disco

The default root password is `'disco'`.

Using:
========
From the docker host, you can ssh into this image as follows:

    $ ssh -l root -p 2200

And now you can run map-reduce jobs.

For more information about docker please visit [docker](https://www.docker.io/).
