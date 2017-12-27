package ossstorage

import (
	"encoding/hex"
	"errors"
	"io"
	"io/ioutil"
	"strings"

	"github.com/denverdino/aliyungo/oss"
	"github.com/facebookgo/rocks-strata/strata"
)

// OSSStorage implements the strata.Storage interface using OSS as its storage backing
type OSSStorage struct {
	oss    *oss.Client
	bucket *oss.Bucket
	region oss.Region
	prefix string
}

func (s *OSSStorage) addPrefix(path string) string {
	return s.prefix + "/" + path
}

func (s *OSSStorage) removePrefix(path string) string {
	return path[len(s.prefix)+1:]
}

// NewOSSStorage initializes the OSSStorage with required OSS arguments
func NewOSSStorage(bucketName string, prefix string, region oss.Region, internal bool, accessKeyID string, accessKeySecret string, secure bool, bucketACL oss.ACL) (*OSSStorage, error) {
	ossclient := oss.NewOSSClient(region, false, accessKeyID, accessKeySecret, secure)
	bucket := ossclient.Bucket(bucketName)

	_, err := bucket.List("", "/", "", 1)

	if err != nil {
		err = bucket.PutBucket(bucketACL)
		if err != nil {
			return nil, err
		}
	}

	return &OSSStorage{
		oss:    ossclient,
		bucket: bucket,
		region: region,
		prefix: prefix,
	}, nil
}

// Get returns a reader to the specified OSS path.
func (s *OSSStorage) Get(path string) (io.ReadCloser, error) {
	path = s.addPrefix(path)
	resp, err := s.bucket.GetResponse(path)
	if resp == nil || err != nil {
		if err.Error() == "The specified key does not exist." {
			err = strata.ErrNotFound(path)
		}
		return nil, err
	}
	etag, found := resp.Header["Etag"]
	if !found {
		return nil, errors.New("No Etag header")
	}
	if len(etag) == 0 {
		return nil, errors.New("Etag header is empty")
	}
	// Note: osstest does not require the trimming, but real OSS does
	checksum, err := hex.DecodeString(strings.TrimSuffix(strings.TrimPrefix(etag[0], "\""), "\""))
	if err != nil {
		return nil, err
	}
	return strata.NewChecksummingReader(resp.Body, checksum), nil
}

// Put places the byte slice at the given path in OSS.
// Put also sends a checksum to protect against network corruption.
func (s *OSSStorage) Put(path string, data []byte) error {
	path = s.addPrefix(path)
	err := s.bucket.Put(path, data, "application/octet-stream", oss.Private)
	return err
}

// PutReader consumes the given reader and stores it at the specified path in OSS.
// A checksum is used to protect against network corruption.
func (s *OSSStorage) PutReader(path string, reader io.Reader) error {
	// TODO(agf): OSS will send a checksum as a response after we do a PUT.
	// We could compute our checksum on the fly by using an ChecksummingReader,
	// and then compare the checksum to the one that OSS sends back. However,
	// goamz does not give us access to the checksum that OSS sends back, so we
	// need to load the data into memory and compute the checksum beforehand.
	// Should fix this in goamz.
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	return s.Put(path, data)
}

// Delete removes the object at the given OSS path
func (s *OSSStorage) Delete(path string) error {
	path = s.addPrefix(path)
	err := s.bucket.Del(path)
	return err
}

// List returns a list of objects (up to maxSize) with the given prefix from OSS
func (s *OSSStorage) List(prefix string, maxSize int) ([]string, error) {
	prefix = s.addPrefix(prefix)
	pathSeparator := ""
	marker := ""

	items := make([]string, 0, 1000)
	for maxSize > 0 {
		// Don't ask for more than 1000 keys at a time. This makes
		// testing simpler because OSS will return at most 1000 keys even if you
		// ask for more, but osstest will return more than 1000 keys if you ask
		// for more. TODO(agf): Fix this behavior in osstest.
		maxReqSize := 1000
		if maxSize < 1000 {
			maxReqSize = maxSize
		}
		contents, err := s.bucket.List(prefix, pathSeparator, marker, maxReqSize)
		if err != nil {
			return nil, err
		}
		maxSize -= maxReqSize

		for _, key := range contents.Contents {
			items = append(items, s.removePrefix(key.Key))
		}
		if contents.IsTruncated {
			marker = s.addPrefix(items[len(items)-1])
		} else {
			break
		}
	}

	return items, nil
}

// Lock is not implemented
func (s *OSSStorage) Lock(path string) error {
	return nil
}

// Unlock is not implemented
func (s *OSSStorage) Unlock(path string) error {
	return nil
}
