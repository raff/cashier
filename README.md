# cashier
Cashier is a caching layer for "conversion services"

## What should it do

- store files in a common storage service, with TTL
  + BadgerDB (DONE)
  + S3 + DynamoDB (DONE)
  + Other cloud stores (TODO)

- Support resumable uploads
  + POST creates a file (DONE)
  + POST fails if file already exists, but notifies client if upload is incomplete (DONE)
  + PUT allows updating the file with remaining bytes, via byte range requests (DONE)

- Support downloads, with range requests (DONE)

- Notify conversion service of new request (TODO)
- After upload, client waits for conversion to complete, maybe with long-poll (TODO)
- When conversion is completed, notify client and start download (TODO)
