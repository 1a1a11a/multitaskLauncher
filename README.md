multitaskLauncher
=================

A clientless tool for submitting a lot of computation-heavy tasks to a pool of
machines/VMs, you don’t need to have any package installed on the computation
node to use this software, it only uses ssh.

### Problem setting 

You have a lot of similar tasks (suppose N tasks) to run and you have a lot of
machines (M machines, N\>M\>\>1) can be used for computation, what’s the easiest
way to run the jobs on the machine pool?

-   ssh into each machine and launch the job, wait and scp the results back?

-   Write a shell script and submit M tasks to M machines, periodically check
    the results and launch new job when old ones finish?

-   No, neither of these approaches is ideal, they take time and can make a
    mess.

 

__multitaskLaucher is such a tool that you only need to write down all the tasks you want to run and where you want to save the results, it will take care of the rest. It will launch the jobs on each worker node, monitor the task running status, retrieve the results when it finishes and launch new task on the node.__

__In addition, it supports checking the task running status and node health status in command line.__

__As one more feature, it will open a http service on port 5002 for easy checking task and node status, so that you can use http://127.0.0.1:5002 for checking the status of each task and each node.__

 

Currently only tested on Ubuntu and macOS, but it should support all \*NIX, the
machine that submits jobs should have python3.5+ installed. And current version
uses ssh heavily, which is not optimized.

 

### Roadmap: 

1.  Add initializer to help user setup pubKey on each worker machine.

2.  Add AWS support, including EC2 and S3.

3.  Use paramiko to replace subprocess run ssh.

 

 

Preparations Needed Before Use 
-------------------------------

Please ensure that you can login into each machine/VM using default pubKey.

 

How to Use 
-----------

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
jason@myMachine:~$ git clone https://github.com/1a1a11a/multitaskLauncher.git 
jason@myMachine:~$ cd multitaskLauncher
edit node and task two files on your environment 
jason@myMachine:~$ ./multitaskLauncher task 
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

 

 
