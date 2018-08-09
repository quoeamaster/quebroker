package main

import (
    "bytes"
    "queutil"
)

// wraps and describes a "Common" error
type CommonError struct {
    StatusCode int
    OptionalDescription string
    Error error
}
// create a new instance of CommonError based on the given information
func NewCommonError (statusCode int, optionalDescription string, err error) *CommonError {
    e := new(CommonError)
    e.StatusCode = statusCode
    e.OptionalDescription = optionalDescription
    e.Error = err
    return e
}
func (c *CommonError) String () string {
    var b bytes.Buffer
    // need to provied the jsonStructure???? (verify)
    b = queutil.BeginJsonStructure(b)
    b = queutil.AddIntToJsonStructure(b, "statusCode", c.StatusCode)
    b = queutil.AddStringToJsonStructure(b, "error", c.Error.Error())
    b = queutil.AddStringToJsonStructure(b, "optionalDescription", c.OptionalDescription)
    b = queutil.EndJsonStructure(b)
    return b.String()
}
