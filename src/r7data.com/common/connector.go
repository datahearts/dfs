package common

import ()

//Connector abstracts the logic which moves data from one processing steps to another
type Connector interface {
	Forward(data interface{}) error
}
