package elastic_v7

import (
	"fmt"
	"github.com/maclon-lee/golanglib/lib/utility"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	logger "github.com/maclon-lee/golanglib/lib/log"
)

// ESLogger implements the estransport.Logger interface.
//
type ESLogger struct {
}

// LogRoundTrip prints the information about request and response.
//
func (l *ESLogger) LogRoundTrip(
	req *http.Request,
	res *http.Response,
	err error,
	start time.Time,
	dur time.Duration,
) error {
	var (
		nReq int64
		nRes int64
	)

	// Count number of bytes in request and response.
	//
	if req != nil && req.Body != nil && req.Body != http.NoBody {
		nReq, _ = io.Copy(ioutil.Discard, req.Body)
	}
	if res != nil && res.Body != nil && res.Body != http.NoBody {
		nRes, _ = io.Copy(ioutil.Discard, res.Body)
	}

	loginfo := fmt.Sprintf("(%s,%d)%s time:%s duration:%f req:%d res:%d", req.Method, res.StatusCode, req.URL.String(), start.Local().Format(utility.FORMATLOGTIME), dur.Seconds(), nReq, nRes)

	// Set error level.
	//
	switch {
	case err != nil:
		logger.Errorf(loginfo)
	case res != nil && res.StatusCode > 0 && res.StatusCode < 400:
		logger.Infof(loginfo)
	case res != nil && res.StatusCode > 399 && res.StatusCode < 500:
		logger.Warnf(loginfo)
	case res != nil && res.StatusCode > 499:
		logger.Errorf(loginfo)
	default:
		logger.Warnf(loginfo)
	}

	return nil
}

// RequestBodyEnabled makes the client pass request body to logger
func (l *ESLogger) RequestBodyEnabled() bool { return true }

// RequestBodyEnabled makes the client pass response body to logger
func (l *ESLogger) ResponseBodyEnabled() bool { return true }