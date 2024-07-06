1. The project is inspired by the NFS,but is in no way related to distributed systems.
2. The point of the project was to understand concurrency and networks properly and be able to design and code up a decently large system based on these concepts within a few weeks.
3. INTRODUCTION-
   - Clients: Clients represent the systems or users requesting access to files within the network file system. They serve as the primary interface to interact with the NFS, initiating various file-related operations such as reading, writing, deleting, and more.
   - Naming Server: The Naming Server stands as a pivotal component in the NFS architecture. This singular instance serves as a central hub, orchestrating communication between clients and the storage servers. Its primary function is to provide clients with crucial information about the specific storage server where a requested file or folder resides. Essentially, it acts as a directory service, ensuring efficient and accurate file access.
   - Storage Servers: Storage Servers form the foundation of the NFS. These servers are responsible for the physical storage and retrieval of files and folders. They manage data persistence and distribution across the network, ensuring that files are stored securely and efficiently.
4. INITIALIZATION-
   - Initialize the Naming Server (NM): The first step is to initialize the Naming Server, which serves as the central coordination point in the NFS. It is responsible for managing the directory structure and maintaining essential information about file locations.
   - Initialize Storage Server 1 (SS_1): Each Storage Server (SS) is responsible for storing files and interacting with both the Naming Server and clients. Initialization of SS_1 involves several sub-steps:
   Upon initialization, SS_1 sends vital details about its existence to the Naming Server. This information includes:

    a. IP address: To facilitate communication and location tracking.
    b. Port for NM Connection: A dedicated port for direct communication with the Naming Server.
    c. Port for Client Connection: A separate port for clients to interact with SS_1.
    d. List of Accessible Paths: A comprehensive list of file and folder paths that are accessible on SS_1.

   - Initialize SS_2 to SS_n: Following the same procedure as SS_1, additional Storage Servers (SS_2 to SS_n) are initialized, each providing their details to the Naming Server.
   - NM Starts Accepting Client Requests: Once all Storage Servers are initialized and registered with the Naming Server, the Naming Server begins accepting client requests. It becomes the central point through which clients access and manage files and directories within the network file system.
5. Adding new storage servers: New Storage Servers (i.e., which begin running after the initial initialization phase) have the capability to dynamically add their entries to the Naming Server at any point during execution. This flexibility ensures that the system can adapt to changes and scaling requirements seamlessly.
6. Client task feedback: Upon completion of tasks initiated by clients, the NM plays a pivotal role in providing timely and relevant feedback to the requesting clients.
7. The client passes READ dir1/dir2/file.txt to the NM -> the NM looks over all the accessible paths in SS1, SS2, SS3… then sees that the path is present in SSx -> The NM gives relevant information about SSx to the client.
8. Reading, Writing, and Retrieving Information about Files:

   - Client Request: Clients can initiate file-related operations such as reading, writing, or obtaining information by providing the file’s path. The NM, upon receiving the request, takes on the responsibility of locating the correct SS where the file is stored.
   - NM Facilitation: The NM identifies the correct Storage Server and returns the precise IP address and client port for that SS to the client.
   - Direct Communication: Subsequently, the client directly communicates with the designated SS. This direct communication is established, and the client continuously receives information packets from the SS until a predefined “STOP” packet is sent or a specified condition for task completion is met. The “STOP” packet serves as a signal to conclude the operation.
9. Creating and Deleting Files and Folders:

   - Client Request: Clients can request file and folder creation or deletion operations by providing the respective path and action (create or delete). Once the NM determines the correct SS, it forwards the request to the appropriate SS for execution.
   - SS Execution: The SS processes the request and performs the specified action, such as creating an empty file or deleting a file/folder.
   - Acknowledgment and Feedback: After successful execution, the SS sends an acknowledgment (ACK) or a STOP packet to the NM to confirm task completion. The NM, in turn, conveys this information back to the client, providing feedback on the task’s status.

10. Error Handling: Defined a set of error codes that can be returned when a client’s request cannot be accommodated. For example, the NM should return distinct error codes for situations where a file is unavailable because it doesn’t exist and when another client is currently writing to the file. Clear and descriptive error codes enhance the communication of issues between the NFS and clients.
11. Efficient Search: Optimize the search process employed by the Naming Server when serving client requests. Avoid linear searches and explore more efficient data structures such as Hashmaps to swiftly identify the correct Storage Server (SS) for a given request. This optimization enhances response times, especially in systems with a large number of files and folders.
12. BOOKKEEPING-
  - Logging and Message Display: Implement a logging mechanism where the NM records every request or acknowledgment received from clients or Storage Servers. Additionally, the NM should display or print relevant messages indicating the status and outcome of each operation. This bookkeeping ensures traceability and aids in debugging and system monitoring.
   -  IP Address and Port Recording: The log should include relevant information such as IP addresses and ports used in each communication, enhancing the ability to trace and diagnose issues.



