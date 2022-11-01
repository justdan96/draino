package utils

import "errors"

func ParseObjects[T any](objs []interface{}) ([]T, error) {
	res := make([]T, 0, len(objs))

	for _, obj := range objs {
		parsed, ok := obj.(T)
		if !ok {
			return nil, errors.New("cannot parse object")
		}
		res = append(res, parsed)
	}

	return res, nil
}
