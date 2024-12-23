# Usage
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

# Documentation

## utils.h
Contains utility functions and definitions used throughout (e.g. `u8`/`i32`/`u64`, `sptr`/`uptr`, `OnDelete` etc.)

## logging.h
- `set_logging` - sets whether logging is enabled
- `log` - logs a message, uses `_mutex_logging` to ensure thread safety
- `prefix_log/norm_log/verbose_log/debug_log/err_log` - logging functions with different prefixes
- macros `throw_if` and `THROW_IF_RECOVERABLE` - used to simplify handling and logging of exceptions and errors.
`throw_if` throws an exception with given string if first boolean is true. `THROW_IF_RECOVERABLE` will either act like `throw_if` if `THROW_ON_RECOVERABLE` compile-time constant is true, but otherwise log the error and execute a given default action and continue without throwing. This is useful in case of recoverable errors that should not crash the program, like a failed connection attempt.

## sockets.h
Contains reimplementation of things from `sys/socket.h` and similar:
- `ipv4_addr` - struct representing an IPv4 address, replaces `struct in_addr`
- `ip_to_string` - function to convert `ipv4_addr` or `struct in_addr` to string 
- `socket_address` - struct representing a socket address, replaces `struct sockaddr_in`
- `socket_address_to_string` - function to convert `socket_address` or `struct sockaddr_in` to string
- `is_valid_socket_address`/`socket_address_from_string` - functions to validate and parse socket addresses
- `getIPAddresses` - function to get all IP addresses of the current machine
- `file_descriptor` - RAII wrapper for file descriptors, replaces `int` and the raw `close`/`send`/`recv`/etc. calls
- `TimeValue` - struct representing a time value, replaces `struct timeval`
    - `to_duration` - converts to `std::chrono::duration`
- several throwing socket initializators, like `create_socket_throw`, `set_socket_options_throw`, `set_timeout_throw`, as well as non-throwing versions

- `ServerSocket` - the first non-trivial class, represents a server socket
    - `file_descriptor socket_fd` - the file descriptor of the socket
    - `const in_port_t port` - the port the socket is bound to
    - On construction creates a new socket and binds it to the specified port, on close closes the socket
    - `get_socket_fd` - returns the file descriptor to be used for accepting
    - `get_port` - returns the port the socket is bound to

## transmission.h
Contains classes and utilities for handling network transmission layers:
- `OpenSocket` - RAII wrapper for socket connections with buffering capabilities
    - `accept_connection` - takes file_descriptor of a local server socket and waits for a connection
    - `connect_to_server` - connects to an external server socket
    - `Send`/`Recv` - send/receive variable amount of data in buffer
- `TransmissionLayer` - handles protocol-level transmission details with built-in batching support
    - `write_byte`/`write_i32`/`write_string`/`write_bytes`/`finalize_send` - write data to buffer
    - `initialize_recv`/`read_byte`/`read_i32`/`read_string`/`read_bytes` - read data from buffer
    - `print_errors`/`reset_error` - error handling
The TransmissionLayer has the following protocol:
- Continuous message byte streams are split into batches (default size 4096)
- First 4 bytes are the int size of the batch
- Next 1 byte is the continuation flag used to indicate if there are more batches in the message byte stream, this is useful when the full size of the whole message is not known, so it does not need to be immediately encoded in the first batch.
- On end of write, `finalize_send` is called to send the last batch and reset the buffer.
- On beginning of read, `initialize_recv` is called to read the size of the batch and initialize the buffer
- New messages begin transparently by calling a write operation again after `finalize_send`, or by calling `initialize_recv` again after some last read operation.
- TransmissionLayer also implements certain connection quality handling via timeout and throughput monitoring and setting appropriate error codes in case of issues.

## nodes.h
This header contains the actual node logic. It is split into the following classes:
- `Node` - a simple base class for all nodes, contains DNS address
- `Server:Node` - base server class which implements a shared basic connection acceptance and thread management framework
- `DNS:Server` - DNS server class, manages master node discovery and node registration
- `DataNode:Server` - Shared class for data nodes and the master node, since they overlap in functionality, and the master node may act as a data node as well.
- `Client:Node` - Client class for uploading and downloading files

### `Server`
`Server` has the following thread management framework:
- `thread_struct` - struct representing a thread, contains a vector of shared pointers to owned connections, a unique pointer to a thread handle, and state management information that allows safely pausing and retrying outgoing connections during synchronization with the new master node.
- `threads` - a hashmap of thread ids to shared pointers to thread structs, this is how running threads are kept track of.
- `connections_data_mutex` - a mutex to ensure thread safety when accessing the threads hashmap and other critical shared data structures on a DataNode that may be accessed by multiple threads.
- `finished_threads` - a vector of unique pointers to handles of threads that have finished their work and are ready to be cleaned up. The main thread that handles the accept loop joins with finished threads every iteration to ensure consistency.
- `finished_threads_mutex` - a mutex to ensure thread safety when accessing the finished threads vector.
- `can_exit_cv` - a condition variable used with `finished_threads_mutex` to signal the main thread that is waiting to exit to let it know to check if no more threads exist in `threads` and it can safely join with all finished threads and exit.
- Newly created threads are typically spawned with `CreateNewThread` and removed with `remove_thread` from the thread itself when it finished the work. The main wrapper of the thread function is the `AutoCloseThreadTemplate` template function which safely handles thrown exceptions and cleans up the thread and its connections in case of an error. The function inside is given the shared pointer to the thread struct, which it uses to access the thread's connections and other data.
`Server` has the following connection management framework which works in tandem with the thread management framework:
- `connection_struct` - struct representing a connection, contains an `OpenSocket` and `TransmissionLayer` instances, thread ownership information, and an optional close request flag.
- `connections` - a hashmap of connection ids to shared pointers to connection structs, this is how active connections are kept track of.
- It is expected that connections added to the `connections` hashmap are also added to the corresponding thread's vector of owned connections. On close of a thread, it will automatically close all its owned connections and remove them from the `connections` hashmap.
- There are `create_new_outgoing_connection` and `remove_connection` functions that are used to establish new client connections and add them to the owner thread vector, and remove specific connections respectively.
- There is a convenient function `CreateAutodeleteOutgoingConnection` with creates a new connection and returns a OnDelete object that will automatically handle removal of the connection once it goes out of scope.
- The main thread accept loop is started with `start` and accepts connections and spawns and configures handler threads. This is done uniquely with `setup_thread`, since unlike in other procedural thread creation cases, here the connection is created before the thread, meaning the previous functions do not apply. However, the functionality is mostly the same.

The main way that custom behaviour is introduces for classes inheriting `Server` is via the virtual function `handle_first_connection`, which is called when a new connection is accepted. This function is expected to handle the initial connection setup and then return on final thread end. Since a thread is allowed to have multiple connections, including 0, it may remove the connection within the handler and continue handling the request asynchronously. Returning from the handler closes the thread and all its remaining connections.

### `DNS`
`DNS` is a class that manages master node discovery and node registration. It has the following key features:
- `addrCurMaster` - the server socket address of the current master node
- `nodes` - an unordered set of socket addresses representing server sockets of all active nodes, which is how the nodes are distinguished (since each node has a unique IP+port combination).
- `mutexDNS` - a mutex to ensure thread safety when accessing the nodes set.
- The four `PACKET_ID` types that the DNS node handles are the following, and mostly self explanatory:
    - `REQUEST_MASTER_ADDRESS` - handles a client request for the master node address
    - `SET_MASTER_ADDRESS` - handles a master node setting its own address
    - `REQUEST_LIST_OF_ALL_NODES` - handles a client request for the list of all active nodes
    - `EDIT_LIST_OF_ALL_NODES` - handles a node setting the list of all active nodes
The DNS node does not spawn any extra asynchronous threads besides the handler threads, as the requests it has to handle are simple and can be quickly handled synchronously.

### `DataNode`
The `DataNode` class is a shared class for data nodes and the master node, since they overlap in functionality, and the master node may act as a data node as well. It is a complicated class and takes up most of the `nodes.h` header. It has the following key features:
- `datanode_mutex` - a mutex to ensure thread safety when accessing any of the variables.
- `node_role` - an enum representing the role of the node, can be `MASTER` or `DATA`. A key point is that DATA functions as a default, has no special functionality that is not enabled in `MASTER` type. What defines if a master node is also a data node is if it treates itself as such (has its own IP in the data node list).
- Data node relevant variables:
    - `this_node_info` - a `node_info` struct, which contains the number of total readers and writers, and a map of file names to `data_file_info` structs. Each `data_file_info` struct contains the number of readers and writers for that file, and the version of that file, which can be used to resolve who has the newest version of the file in case of a conflict (currently not implemented due to lack of replication).
    - `expected_client_requests` - a map of client request IDs to `expected_client_entry` structs, which contain the file name and the expected operation type (read/write) of the request. This is used to ensure that the client does not send a request to a different node than the one it started the request with, and that the request arrives in time or else it gets cancelled and a notification is sent to the master node from the data node. Master node lacks a way to tell when the read or write operation has fully completed, so the data node effectively keeps the lock until it sends a completion message to the master node.
    - `is_backup` - a boolean that is set to true if the node is designated as a backup node. The backup node is the one that will take over as the master node if it detects the master node failure via heartbeat error. The backup node is designated by the master node and iniated via a `SET_DATA_NODE_AS_BACKUP` packet.
- Master node relevant variables:
    - `file_to_info` - a map of file names to `master_file_info` structs, which contain the number of readers and writers for that file, and a set of socket addresses representing the replicas of that file. This supports multiple replicas, but currently only one replica is used, and the replication is not implemented. These are used to decide if an operation should be denied because it would conflict with ongoing operations on the same file.
    - `node_to_info` - a map of socket addresses to `node_info` structs, which contain the number of total readers and writers for that node, and a map of file names to `data_file_info` structs. This is used to keep track of the state of all data nodes, and coordinate the file operations between them.
    - `backup_node` - a socket address representing the backup node, which is the node that will take over as the master node if the master node fails. The backup node is designated by the master node and iniated via a `SET_DATA_NODE_AS_BACKUP` packet.
- The following are some of mostly self-explanatory functions defined:
    - `random_non_master_node` - returns a random non-master node from the list of all nodes, useful in backup node selection
    - `random_node` - returns a random node from the list of all nodes, useful in initial data node selection to contain a new file
    - `clear_timeout_client_requests` - used by data node to periodically check and cancel any awaited client requests that have timed out
- The first important function is `setup_master`, which initializes the master node state, updates the DNS, configures the data nodes if needed, and starts the heartbeats. The heartbeats are used to detect if a data node has failed, and if the master node has failed, the backup node will take over as the master node. This may be called either in the beginning by the initial master node, in which case it will wait until the end of the `Server::start` function, or by the backup node after detecting the master node failure, in which case it will also first gather information about the file system state from the nodes, and then resume their operation.
- `_setup_heartbeat` is the function spawned in a new thread that initializes a connection with each data node from the master node. On detecting failure, it executes `_handle_heartbeat_error` which will remove the failed node from the list of active nodes and pick a new backup node if the master node has failed.
- `handle_first_connection` handles the following `PACKET_ID` types:
    - `HEARTBEAT` - initiates the heartbeat response. If the node is a backup node, it will also become a new master node if the heartbeat fails.
    - `SET_MASTER_ADDRESS` - updates the master node addresss
    - `SET_DATA_NODE_AS_BACKUP` - designates the node as a backup node
    - `CLIENT_TO_MASTER_CONNECTION_REQUEST` - processes initial client connection and either allows it (first notifying the corresponding data node) or denies it
    - `CLIENT_TO_DATA_CONNECTION_REQUEST` - handles the client request to read or write a file, upon finishing (or failing) the operation, it sends a completion message to the master node to release the lock in the form of the number of readers and writers for that file.
    - `MASTER_NODE_TO_DATA_NODE_ADD_NEW_CLIENT` - notifies the data node that a new client connection is inbound
    - `DATA_NODE_TO_MASTER_CONNECTION_REQUEST` - notifies the master node that the client connection has ended, releasing the lock in the form of the number of readers and writers for that file.
    - `MASTER_TO_DATA_NODE_INFO_REQUEST` - requests the data node to send its state information, used by the backup node to gather the file system state in case of master node failure.
    - `MASTER_TO_DATA_NODE_PAUSE_CHANGE` - used to pause or resume the data node activity, used by the backup node to effectively perform a distributed snapshot sequence during which it can safely request the information from the data nodes and handle the master node initialization. This is mostly implemented but the certainly could be edge cases where the PAUSE behaviour should apply to more connection types than it currently does (e.g. during replication).

Almost all of the messages used within these handlers have a shared structure, which appends a byte to mark whose turn it is in the dialogue. This allows sending more messages one after another and not have to start a new connection for each message. 
### `Client`
The client is a simple class that does read or write operations against the file system. It does the following sequence in both cases:
- Connect to the DNS node to get the master node address
- Connect to the master node to request the file operation and get the data node address (or have the operation be denied)
- Connect to the data node to perform the file operation (if allowed)

## main.cpp
Main mostly contains the argument parsing and the handler which initializes the correct node type based on the arguments. To understand the arguments use -h or --help or simply run the program without arguments.