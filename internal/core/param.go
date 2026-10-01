// internal/core/param.go
package core

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
)

var (
	ErrInvalidParamValue    = errors.New("invalid param value")
	ErrUnsupportedParamType = errors.New("unsupported param type")
)

type ParamKind uint8

const (
	KindInvalid ParamKind = iota
	KindNum
	KindStr
	KindBool
)

type ParamValue struct {
	kind ParamKind
	num  float64
	str  string
	b    bool
}

func NumFloat(v float64) ParamValue { return ParamValue{kind: KindNum, num: v} }
func NumInt(v int64) ParamValue     { return ParamValue{kind: KindNum, num: float64(v)} }
func Str(v string) ParamValue       { return ParamValue{kind: KindStr, str: v} }
func Bool(v bool) ParamValue        { return ParamValue{kind: KindBool, b: v} }

func (p ParamValue) Kind() ParamKind { return p.kind }

func (p ParamValue) AsFloat() (float64, bool) { return p.num, p.kind == KindNum }
func (p ParamValue) AsInt() (int64, bool)     { return int64(p.num), p.kind == KindNum }
func (p ParamValue) AsStr() (string, bool)    { return p.str, p.kind == KindStr }
func (p ParamValue) AsBool() (bool, bool)     { return p.b, p.kind == KindBool }

func (p ParamValue) String() string {
	switch p.kind {
	case KindNum:
		return strconv.FormatFloat(p.num, 'g', -1, 64)
	case KindStr:
		return p.str
	case KindBool:
		return strconv.FormatBool(p.b)
	default:
		return "<invalid>"
	}
}

func (p ParamValue) MarshalJSON() ([]byte, error) {
	switch p.kind {
	case KindNum:
		return json.Marshal(p.num)
	case KindStr:
		return json.Marshal(p.str)
	case KindBool:
		return json.Marshal(p.b)
	default:
		return nil, ErrInvalidParamValue
	}
}

func (p *ParamValue) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case float64:
		*p = NumFloat(t)
	case int:
	case int64:
	case uint:
	case uint64:
		*p = NumInt(int64(t))
	case string:
		*p = Str(t)
	case bool:
		*p = Bool(t)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupportedParamType, v)
	}
	return nil
}
