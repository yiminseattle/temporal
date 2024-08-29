// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package predicates

import (
	"go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
)

type (
	EmptyImpl[T any] struct{}
)

var EmptyPredicateProtoSize = (&persistencespb.Predicate{
	PredicateType: enums.PREDICATE_TYPE_EMPTY,
	Attributes: &persistencespb.Predicate_EmptyPredicateAttributes{
		EmptyPredicateAttributes: &persistencespb.EmptyPredicateAttributes{},
	},
}).Size()

func Empty[T any]() Predicate[T] {
	return &EmptyImpl[T]{}
}

func (n *EmptyImpl[T]) Test(t T) bool {
	return false
}

func (n *EmptyImpl[T]) Equals(
	predicate Predicate[T],
) bool {
	_, ok := predicate.(*EmptyImpl[T])
	return ok
}

func (*EmptyImpl[T]) Size() int {
	return EmptyPredicateProtoSize
}
