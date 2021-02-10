The first step in our Big-Data journey is setting up Docker. 
I have it set up on Windows 10, using [WSL](https://docs.microsoft.com/en-us/windows/wsl/) (Windows Subsystem for Linux) with Ubuntu 20.04.
I am running all this on a laptop with an i7-7700HQ cpu and 16GB of system memory, which should be more than sufficient for what we will be doing.

Once Docker is installed, we can try it out with a simple Scala program. We do this by first opening a Docker container that comes with the complete Scala environment pre-installed. In the virtual machine (VM), we type:

```
$ docker run -it williamyeh/scala bash
```

This will download the image automatically and opens it with bash to that we can work within the it.

In our VM we can now create a new Scala file. Let's start with a simple `Hello, World!' program, which we will place in a new file ```helloWorld.scala```.

```
object HelloWorld {
    def main(args: Array[String]): Unit = {
        println("Hello, World!")
    }
}
```

We can then compile and run this simple program using the following commands, which should print a message to the console.

```
# scalac helloWorld.scala
# scala -classpath . HelloWorld
Hello, World!
```
