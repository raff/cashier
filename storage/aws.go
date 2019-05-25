/*
This package stores files in AWS S3 (and metadata in DynamoDB),
allowing for incremental writes of multiple of BlockSize.
For each file it stores a "metadata" record and a series of "block" records.
Files and data expire after a predefined TTL.
*/
package storage

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/awserr"
	"github.com/aws/aws-sdk-go-v2/aws/external"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// An instance of the Storage service based on AWS S3

type awsStorage struct {
	db     *dynamodb.Client
	store  *s3.Client
	bucket string // bucket is also used as the table name in DynamoDB
	prefix string
	ttl    time.Duration
}

// Open data folder and return instance of storage service
func OpenAWS(dataFolder string, ttl time.Duration) (*awsStorage, error) {

	var prefix string
	parts := strings.SplitN(dataFolder, "/", 2)
	bucket := parts[0]
	if len(parts) > 1 {
		prefix = parts[1]
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
	}

	cfg, err := external.LoadDefaultAWSConfig()
	if err != nil {
		return nil, err
	}

	db := dynamodb.New(cfg)

	_, err = db.DescribeTableRequest(&dynamodb.DescribeTableInput{
		TableName: aws.String(bucket),
	}).Send(context.TODO())

	if err != nil {
		return nil, err // table does not exist ?
	}

	store := s3.New(cfg)

	_, err = store.GetBucketLocationRequest(&s3.GetBucketLocationInput{
		Bucket: aws.String(prefix),
	}).Send(context.TODO())

	if err != nil {
		return nil, err // table does not exist ?
	}

	return &awsStorage{db: db, store: store, bucket: bucket, prefix: prefix, ttl: ttl}, nil
}

// Close storage service
func (s *awsStorage) Close() error {
	s.db = nil
	return nil
}

// Run garbage collector
func (s *awsStorage) GC() error {
	return nil
}

func Nint(s *string) int64 {
	n, _ := strconv.Atoi(*s)
	return int64(n)
}

func intN(n int64) *string {
	return aws.String(strconv.Itoa(int(n)))
}

func (s *awsStorage) upsertInfo(key string, value *info, create bool) error {
	var cond *string

	data, _ := value.MarshalString()
	if create {
		cond = aws.String("attribute_not_exists(Id)")
	}

	_, err := s.db.PutItemRequest(&dynamodb.PutItemInput{
		Item: map[string]dynamodb.AttributeValue{
			"Id": {
				S: aws.String(infoKey(key)),
			},
			"Value": {
				S: aws.String(data),
			},
			"TTL": {
				N: intN(time.Now().Add(s.ttl).Unix()),
			},
		},
		ConditionExpression:         cond,
		ReturnConsumedCapacity:      dynamodb.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValueNone,
		TableName:                   aws.String(s.bucket),
	}).Send(context.TODO())

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return ErrExists
			}
		}
	}

	return err
}

func (s *awsStorage) getInfo(key string) (*info, error) {
	key = infoKey(key)

	res, err := s.db.GetItemRequest(&dynamodb.GetItemInput{
		ConsistentRead: aws.Bool(true),
		Key: map[string]dynamodb.AttributeValue{
			"Id": {
				S: aws.String(key),
			},
		},
		ReturnConsumedCapacity: dynamodb.ReturnConsumedCapacityNone,
		TableName:              aws.String(s.bucket),
	}).Send(context.TODO())

	if err != nil {
		return nil, err
	}

	if res.Item == nil {
		return nil, ErrNotFound
	}

	var fileInfo info
	if err = (&fileInfo).UnmarshalString(aws.StringValue(res.Item["Value"].S)); err != nil {
		log.Println("Key:", key, "Item:", res)
		return nil, err
	}

	fileInfo.ExpiresAt = time.Unix(Nint(res.Item["TTL"].N), 0)
	return &fileInfo, nil
}

// Create new file, by adding the file info
func (s *awsStorage) CreateFile(key, filename, ctype string, size int64, hash []byte) error {
	return s.upsertInfo(key,
		&info{Name: filename, ContentType: ctype, Length: size, Hash: toHex(hash[:])}, true)
}

// Delete file
func (s *awsStorage) DeleteFile(key string) error {
	ikey := infoKey(key)

	_, err := s.db.DeleteItemRequest(&dynamodb.DeleteItemInput{
		Key: map[string]dynamodb.AttributeValue{
			"Id": {
				S: aws.String(key),
			},
		},
		ReturnConsumedCapacity:      dynamodb.ReturnConsumedCapacityNone,
		ReturnItemCollectionMetrics: dynamodb.ReturnItemCollectionMetricsNone,
		ReturnValues:                dynamodb.ReturnValueNone,
		TableName:                   aws.String(s.bucket),
	}).Send(context.TODO())
	if err != nil {
		return err
	}

	// here we should delete the S3 blocks
	req := s.store.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket:     aws.String(s.bucket),
		Prefix:     aws.String(s.prefix),
		StartAfter: aws.String(prefixKey(key)),
	})

	var dels s3.Delete

	p := s3.NewListObjectsV2Paginator(req)

	for p.Next(context.TODO()) {
		page := p.CurrentPage()

		for _, obj := range page.Contents {
			dels.Objects = append(dels.Objects, s3.ObjectIdentifier{Key: obj.Key})
		}
	}

	if err := p.Err(); err != nil {
		if len(dels.Objects) == 0 {
			return err
		}

		log.Println(err)
	}

	if len(dels.Objects) == 0 {
		return nil
	}

	_, err = s.store.DeleteObjectsRequest(&s3.DeleteObjectsInput{
		Bucket: aws.String(s.bucket),
		Delete: &dels,
	}).Send(context.TODO())

	// should check for list of Errors in DeleteObjectOutput
	if err != nil {
		log.Println("error deleting S3 %v: %v", ikey, err)
	}

	return nil
}

// Add data to file
func (s *awsStorage) WriteAt(key string, pos int64, data []byte) (int64, error) {
	if pos < 0 {
		return InvalidPos, ErrInvalidPos
	}

	nblocks, rest := len(data)/BlockSize, len(data)%BlockSize
	startBlock, rr := int(pos/BlockSize), int(pos%BlockSize)
	if rr != 0 {
		log.Println(key, "pos", pos, "block", startBlock, "rest", rr)
		return InvalidPos, ErrInvalidPos
	}

	retpos := InvalidPos

	fileInfo, err := s.getInfo(key)
	if err != nil {
		return InvalidPos, err
	}

	//log.Println(fileInfo, "start", startBlock, "blocks", nblocks, "rest", rest, "pos", pos)

	if fileInfo.CurPos < 0 { // file complete
		return InvalidPos, ErrExists
	}

	if pos != fileInfo.CurPos { // wrong start
		log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "cur", fileInfo.CurPos)
		return InvalidPos, ErrInvalidPos
	}

	if pos+int64(len(data)) > fileInfo.Length { // out of boundary
		log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "data", len(data), "file", fileInfo.Length)
		return InvalidPos, ErrInvalidSize
	}

	fblocks := int(fileInfo.Length / BlockSize)

	if startBlock+nblocks < fblocks && rest != 0 {
		log.Println(fileInfo.Name, "block", startBlock, "pos", pos, "n", nblocks, "file", fblocks, "rest", rest)
		return InvalidPos, ErrInvalidSize
	}

	if pos+int64(len(data)) == fileInfo.Length && rest > 0 {
		nblocks += 1
	}

	block := startBlock
	offs := int64(0)
	ldata := len(data)

	curHash := getHasher()
	if err := unmarshalHash(curHash, fileInfo.CurHash); err != nil {
		return InvalidPos, err
	}

	for ldata > 0 {
		bkey := blockKey(key, block)
		buf := data[offs:]
		if len(buf) > BlockSize {
			buf = buf[:BlockSize]
		}

		_, err := s.store.PutObjectRequest(&s3.PutObjectInput{
			Body:    bytes.NewReader(buf),
			Bucket:  aws.String(s.bucket),
			Key:     aws.String(s.prefix + bkey),
			Expires: aws.Time(time.Now().Add(s.ttl)),
		}).Send(context.TODO())

		if err != nil {
			return InvalidPos, err
		}

		curHash.Write(buf)

		block += 1
		offs += int64(len(buf))
		ldata -= len(buf)
	}

	hh := curHash.Sum(nil)
	if fileInfo.CurPos+offs == fileInfo.Length { // we are done
		if fileInfo.Hash == "" {
			fileInfo.Hash = toHex(hh)
		} else if fileInfo.Hash != toHex(hh) {
			// delete file ?
			return InvalidPos, ErrInvalidHash
		}

		retpos = FileComplete
		fileInfo.CurPos = FileComplete
		fileInfo.CurHash = ""
	} else {
		fileInfo.CurHash, err = marshalHash(curHash)
		if err != nil {
			return InvalidPos, err
		}

		fileInfo.CurPos += offs
		retpos = fileInfo.CurPos
	}

	fileInfo.Created = time.Now()
	return retpos, s.upsertInfo(key, fileInfo, false)
}

func (s *awsStorage) ReadAt(key string, buf []byte, pos int64) (int64, error) {
	if pos < 0 {
		return 0, ErrInvalidPos
	}

	block, offs := pos/BlockSize, pos%BlockSize
	nread := int64(0)

	fileInfo, err := s.getInfo(key)
	if err != nil {
		return 0, err
	}

	if fileInfo.CurPos != FileComplete {
		return 0, ErrIncomplete
	}

	if pos > fileInfo.Length {
		return 0, ErrInvalidPos
	}

	lbuf := len(buf)
	if int(fileInfo.Length-pos) < lbuf {
		lbuf = int(fileInfo.Length - pos)
	}

	rrange := ""
	if offs > 0 {
		rrange = fmt.Sprintf("bytes=%v-", offs)
		if lbuf < int(BlockSize-offs) {
			rrange += strconv.Itoa(int(offs) + lbuf - 1)
		}
	}

	readn := BlockSize
	for p := 0; lbuf > 0; block += 1 {
		bkey := blockKey(key, int(block))

		res, err := s.store.GetObjectRequest(&s3.GetObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(s.prefix + bkey),
			Range:  aws.String(rrange),
		}).Send(context.TODO())

		rrange = ""

		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				if aerr.Code() == s3.ErrCodeNoSuchKey {
					return 0, ErrNotFound
				}

				return 0, err
			}
		}

		if readn > lbuf {
			readn = lbuf
		}

		n, err := io.ReadAtLeast(res.Body, buf[p:], readn)
		if err != nil {
			if err == io.EOF && n == lbuf {
				err = nil
			} else {
				return 0, err // if we read something, should we return it ?
			}
		}

		res.Body.Close()

		nread += int64(n)
		lbuf -= n
		p += n
	}

	return nread, nil
}

// Return file info
func (s *awsStorage) Stat(key string) (*FileInfo, error) {
	var stats *FileInfo

	fileInfo, err := s.getInfo(key)
	if err != nil {
		return nil, err
	}

	stats = &FileInfo{
		Name:        fileInfo.Name,
		ContentType: fileInfo.ContentType,
		Created:     fileInfo.Created,
		Hash:        fileInfo.Hash,
		Length:      fileInfo.Length,
		Next:        fileInfo.CurPos,
		ExpiresAt:   fileInfo.ExpiresAt,
	}

	return stats, nil
}

// Scan database, for debugging purposes
func (s *awsStorage) Scan(start string) error {
	dbReq := s.db.ScanRequest(&dynamodb.ScanInput{
		TableName: aws.String(s.bucket),
		Select:    dynamodb.SelectAllAttributes,
	})

	dbPaginate := dynamodb.NewScanPaginator(dbReq)

	var records []struct {
		Id    string
		Value string
		TTL   int64
	}

	fmt.Println("Records:")

	for dbPaginate.Next(context.TODO()) {
		if err := dynamodbattribute.UnmarshalListOfMaps(dbPaginate.CurrentPage().Items, &records); err != nil {
			log.Println("cannot unmarshal items:", err)
			break
		}

		for _, r := range records {
			fmt.Printf(" %s: size=%v expires=%v\n", r.Id, len(r.Value), time.Unix(r.TTL, 0))
		}
	}

	err := dbPaginate.Err()
	if err != nil {
		return err
	}

	fmt.Println("Blocks:")

	storeReq := s.store.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(s.prefix),
	})

	storePaginate := s3.NewListObjectsV2Paginator(storeReq)

	for storePaginate.Next(context.TODO()) {
		for _, obj := range storePaginate.CurrentPage().Contents {
			fmt.Printf(" %s: size=%v expires=%v\n",
				aws.StringValue(obj.Key),
				aws.Int64Value(obj.Size),
				aws.TimeValue(obj.LastModified).Add(s.ttl))
		}
	}

	return storePaginate.Err()
}
