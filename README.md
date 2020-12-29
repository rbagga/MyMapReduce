# Custom Map Reduce
Contributers: Ayushi Singh, Sam Pal, and Rishu Bagga

## Overview
For this project, we run `threaded.go` on each machine, which acts as both a client (to send its membership list) and server (to continuously listen). We have one introducer machine and the rest are worker machines. Once all machines have joined the same membership list, any machine can invoke a `maple` task (which is similar to map) and a `juice` task (which is similar to reduce). To run these commands, they must be specified as such: <br>
<br>`maple <maple_exe> <num_maples> <sdfs_intermediate_filename_prefix> <sdfs_src_directory>`</br>
<br>`juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1}` </br>
<br>

As before, users can run command-line arguments into each machine in order to retrieve more information about each machine and its behavior and see the log files for each machine (named as `log-{machine's IP address:port}` that track changes of global flags (ex. switching, leaving) and actions of each machine (ex. joining). 

## Set up SSH
1. On all VMs:
```
mv authorized_keys ~/.ssh/
exec ssh-agent bash
chmod 400 sshprivatekey
ssh-add sshprivatekey 
```

## Run Instructions

We hardcoded the ip_address of the introducer in threaded.go, so be sure to specify the desired port in that file and when running the introducer (explicit steps are listed below)

In threaded.go, specify your desired introducer port:
```
var introducer = "{introducer port #}" // hardcoded to our VM
```

To run the introducer, specify the introducer port number and the parameter `1`, which signifies the introducer flag.
```
go run threaded.go {introducer port #} 1
```

To run other machines that are not the introducer, specify the port number and the parameter `0`, which signifies that it is not the introducer.
```
go run threaded.go {port #} 0
```

## MapleJuice

### Run condorcet application: 
```
maple condorcet_map1.go <num_maples> <map1_prefix> input
juice condorcet_reduce1.go <num_juices> <map1_prefix> <dest1_file> delete_input={0, 1}
maple condorcet_map2.go <num_maples> <map2_prefix> <dest1_file>
juice condorcet_reduce2.go <num_juices> <map2_prefix> <dest2_file> delete_input={0, 1}
```

### Run COVID contact tracing application: 
```
maple covid_map1.go <num_maples> <map1_prefix> covid_D1.txt 
juice covid_reduce1.go <num_juices> <map1_prefix> <dest1_file> delete_input={0, 1}
maple covid_map2.go <num_maples> <map2_prefix> <dest1_file>
juice covid_reduce1.go <num_juices> <map2_prefix> <dest2_file> delete_input={0, 1}
dest1_file >> combined_dest 
dest2_file >> combined_dest
maple covid_map3.go <num_maples> <map3_prefix> combined_dest
juice covid_reduce1.go <num_juices> <map3_prefix> <dest3_file> delete_input={0, 1}
maple covid_map4.go <num_maples> <map4_prefix> <dest3_file>
juice covid_reduce4.go <num_juices> <map4_prefix> <dest4_file> delete_input={0, 1}
```

## Go

### To run the condorcet application, 
```
go run condorcet_map1.go voter_input.txt     // first map
go run condorcet_reduce1.go {M1 output file} // first reduce
go run condorcet_map2.go {R1 output file}    // second map
go run condorcet_second1.go {M2 output file} // second reduce
```

### To run the COVID contact tracing application
```
go run covid_map1.go covid_D1.txt         // first map
go run covid_reduce1.go {M1 output file}  // first reduce
go run covid_map2.go covid_D2.txt         // second map
go run covid_reduce2.go {M2 output file}  // second reduce
go run covid_map3.go {R1, R2 output file} // third map
go run covid_reduce3.go {M3 output file}  // third reduce
go run covid_map4.go {R3 output file}     // fourth map
go run covid_reduce4.go {M4 output file}  // fourth reduce
```

## Command-Line Arguments

In order to specify actions from the command line while these machines are running, use
* `list`: list machine's membership list
* `listself`: list machine's IP address
* `switch`: switch between gossip-style heartbeating and all-to-all (gossip by default)
* `leave`: machine leaves membership list
* `put localfilename sdfsfilename`: from local dir
* `get sdfsfilename localfilename`: fetches to local dir
* `delete sdfsfilename`: delete file from SDFS
* `ls sdfsfilename`:  list all machine (VM) addresses where this file is currently being stored
* `store`: at any machine, list all files currently being stored at this machine

