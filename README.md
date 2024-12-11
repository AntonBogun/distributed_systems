0. Compile
> make

1. Find out IP on each machine
`python3 get_ip.py`

1. Initialize DNS
`./bin/main -mode dns -p 8089`

2. Initialize Data nodes with dns ip and port
`./bin/main -mode data -p 8090 -dns 128.214.9.26:8089`

`./bin/main -mode data -p 8091 -dns 128.214.9.26:8089`

3. Initialize Master with dns ip and port, and each data node's ip and port. The master node can be also a data node.
`./bin/main -mode master -p 8092 -dns 128.214.9.26:8089 -data_nodes 128.214.9.25:8090 128.214.11.91:8091 128.214.9.26:8092`

4. Upload and download files with client
`./bin/main -mode client -dns 128.214.9.26:8089 --upload file1 --file monument.jpg`

`./bin/main -mode client -dns 128.214.9.26:8089 --download file1 --file monument2.jpg`

The two commands can be run from different machines.

The master node may crash between upload and download and it will still resolve the download correctly if the file wasn't uploaded to the master node.