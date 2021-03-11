
Operation cache:

A cache that given a key based on one or more of:
	resource version/hash, operation body, user id, resource name
	
Can store:
	the original resource body and operation (in case the operation fails, it can be retried
	without re-uploading the "source")
	
	the operation result and "destination" resource body (in case the client gets
	disconnected before the operation completes, or before competed downloading
	the "destination" resource, it can simply download the result)
	
The cache could potentially store the original resource separately from the requested
operation if there is a use case for executing different operations for the same resource.

The cache could also potentially implement block or resumable uploads, so that a client
that gets disconnected while uploading a large file could later resume the operation
from the point it got interruputed.

How to access the cache:

1) Obvious way:
	Add a new API to check the cache. If there is a hit, use the results
	
	Pros: easy to do
	Cons: extra call
	
2) "Tricky" way:
	Plug the cache in the upload/convert flow and "abort" the upload if not needed.
	This can be done by making the upload API accept "Expect: 100-continue", having
	the client issue the 100-continue (with some extra info in the request header) and 
	stop the upload if there is a cache hit.
	
	Pros: save a call, possibly 2 (if we also implement upload+convert)
	Cons: the client needs to support the "100 Continue" protocol (upload request header,
		wait for 100-Continue response, stop upload if receive different code)


Architecture:

A cache entry is composed of:

- Cache key: a unique key that identifies the input resource and operation
- input resource: the file to be converted
	- file content
	- file size (total)
	- file hash (final file hash)
	- current file size (or upload position)
	- current file hash
- operation: an object describing the conversion operation
	- if we are supporting only one type of operation this is not needed
- output resource: a file representing the result of the operation
	- file content
	- file size (total)
	- file hash (final file hash)
- a "condition" object for the client to wait on

Steps:

1) Calculate a key for the new resource and operation
- this could potentially done at the client level, i.e. the client is responsible
	to calculate a unique key that identifies a tuple (resource + operation)
- if there is no need to support caching for multiple clients (i.e. multiple clients
	uploading the same file), then the key can be just a unique ID (UUID)
- if we want to support caching for same resource on multiple clients, then the
	key should be at the minimum a hash of the input file content + operation.
	
2) Upload file
- the client issue a POST on the cache endpoint, for the specified key 
	and starts uploading the file
- as soon as the upload start the cache entry is marked as "UPLOADING" and set the TTL
- we may want to support resumable uploads (if the upload stops the client can ask
  for current position and continue from there
- when the upload completes, the cache entry is marked as "PROCESSING" and an event
	is sent to the operation processor
	
3) Wait for completion
- After the file is uploaded and the operation is triggered the client
	will wait on the operation completion
- If the client gets disconnected, it can re-connect to the cache issuing a GET on
	the cache endpoint, for the specified key. This will make the client go back
	to wait on the condition variable for that key
- When the operation completes, the service will change the cache entry status
	to "COMPLETE" and start sending the results.
	
4) Retrieve results
		

- file key
	{key}
- info record
   	{}-info
- blocks record ?

- blocks
	{key}-{block-number}


-----

- increase throughput (parallel uploads):
	- allow multiple clients to upload pieces of the file (don't check for current position)
	- how do you know when the file is complete ?
            - store a map of uploaded blocks ? (can be a bitmap, can be a count ?)
                - if the user calculates the "commutative hash" of the input file, we can compare that with the current hash value (see next)
        - how do you know the file is "correct" ?
            - use a "commutative hash" instead of standard hash:
                - standard hash must be computed in the correct order
                - a commutative hash is just the sum of partial hashes (hash of 1 block), the result is the same independent on the order.
	
- progressive responses
	- server send response chunks BEFORE the client complete the upload
		- this require an "async" client that can "read" from the socket while
			upload
		- completely non-standard (for HTTP)
		
	- client upload single chunk, server respond with acknowled (chunk received) OR
		partial response (some previous chunk was processed and this is the response)
		- this should be easy to implement for any client
		- may not work in conjunction with "parallel" downloads (chuncs' uploads may not be
			sequential, not clear if clients can reorder the chunked responses)
