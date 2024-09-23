package gcp

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type StorageClient struct {
	client *storage.Client
	bucket string
}

func NewStorageClient(client *storage.Client, bucket string) *StorageClient {
	return &StorageClient{
		client: client,
		bucket: bucket,
	}
}

func (s *StorageClient) Reader(ctx context.Context, object string) (io.Reader, error) {
	return s.client.Bucket(s.bucket).Object(object).NewReader(ctx)
}

func (s *StorageClient) Writer(ctx context.Context, object string) io.WriteCloser {
	return s.client.Bucket(s.bucket).Object(object).NewWriter(ctx)
}
