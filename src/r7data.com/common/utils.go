package common

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"strconv"
)

func GenerateRandomId(length, maxRetry int) (string, error) {
	numOfRetry := 0
	var err error
	for {
		rb := make([]byte, length)
		_, err := crand.Read(rb)

		if err != nil {
			if numOfRetry < maxRetry {
				numOfRetry++
			} else {
				break
			}
		} else {
			id := base64.URLEncoding.EncodeToString(rb)
			return id, nil
		}
	}

	return "", fmt.Errorf("Error generating Id after %v retries. err=%v", numOfRetry, err)
}

func GenerateUUID() uint64 {
	var newId uint64
	binary.Read(crand.Reader, binary.LittleEndian, &newId)
	return newId
}

func NormalizeUrl(url string, port int) string {
	var serverURL string
	if !strings.HasPrefix(url, "http://") {
		serverURL = "http://" + url
	} else {
		serverURL = url
	}

	if portIndex := strings.LastIndex(serverURL, ":"); portIndex == -1 || portIndex == 4 {
		serverURL += ":" + strconv.Itoa(port)
	}

	return serverURL
}
