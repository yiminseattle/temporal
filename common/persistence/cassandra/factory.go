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

package cassandra

import (
	"fmt"
	"sync"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends datastore implementations backed by cassandra
	Factory struct {
		sync.RWMutex
		cfg              config.Cassandra
		clusterName      string
		logger           log.Logger
		execStoreFactory *executionStoreFactory
		session          gocql.Session
	}

	executionStoreFactory struct {
		session gocql.Session
		logger  log.Logger
	}

	MultiCassFactory struct {
		*Factory // primary factory
		Factories []*Factory
	}
)

func NewMultiCassFactory(
	cfgs []config.Cassandra,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
) *MultiCassFactory {

	var factories []*Factory
	for _, cfg := range cfgs {
		factories = append(factories, NewFactory(cfg, r, clusterName, logger))
	}

	multiCassFactory := &MultiCassFactory{
		Factory: factories[0],
		Factories: factories,
	}

	return multiCassFactory
}

//func (f *MultiCassFactory) Close() {
//	f.Factory.Close()
//}
//func (f *MultiCassFactory) NewTaskStore() (p.TaskStore, error) {
//	return f.Factory.NewTaskStore()
//}

type multiCassShardStore struct {
	stores []p.ShardStore
}

func (f *MultiCassFactory) NewShardStore() (p.ShardStore, error) {
	var stores []p.ShardStore
	for _, factory := range f.Factories {
		store, err := factory.NewShardStore()
		if err != nil {
			return nil, err
		}
		stores = append(stores, store)
	}
	return &multiCassShardStore{
		stores: stores,
	}, nil
}

func (s *multiCassShardStore) Close() {
	for _, store := range s.stores {
		store.Close()
	}
}

func (s *multiCassShardStore) GetName() string {
	return s.stores[0].GetName()
}
func (s *multiCassShardStore) GetClusterName() string {
	return s.stores[0].GetClusterName()
}

func (s *multiCassShardStore) getStore(shardID int32) p.ShardStore {
	idx := shardID % int32(len(s.stores))
	return s.stores[idx]
}

func (s *multiCassShardStore) CreateShard(request *p.InternalCreateShardRequest) error {
	return s.getStore(request.ShardID).CreateShard(request)
}
func (s *multiCassShardStore) GetShard(request *p.InternalGetShardRequest) (*p.InternalGetShardResponse, error) {
	return s.getStore(request.ShardID).GetShard(request)
}
func (s *multiCassShardStore) UpdateShard(request *p.InternalUpdateShardRequest) error {
	return s.getStore(request.ShardID).UpdateShard(request)
}


//func (f *MultiCassFactory) NewHistoryStore() (p.HistoryStore, error) {
//	return f.Factory.NewHistoryStore()
//}
//func (f *MultiCassFactory) NewMetadataStore() (p.MetadataStore, error) {
//	return f.Factory.NewMetadataStore()
//}
func (f *MultiCassFactory) NewExecutionStore(shardID int32) (p.ExecutionStore, error) {
	idx := shardID % int32(len(f.Factories))
	fmt.Printf("**** creating execution store for shardID: %d, using idx %d session.\n", shardID, idx)

	factory := f.Factories[idx]
	return factory.NewExecutionStore(shardID)
}

//func (f *MultiCassFactory) NewVisibilityStore() (p.VisibilityStore, error) {
//	return f.Factory.NewVisibilityStore()
//}
//func (f *MultiCassFactory) NewQueue(queueType p.QueueType) (p.Queue, error) {
//	return f.Factory.NewQueue(queueType)
//}
//func (f *MultiCassFactory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
//	return f.Factory.NewClusterMetadataStore()
//}


// NewFactory returns an instance of a factory object which can be used to create
// datastores that are backed by cassandra
func NewFactory(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
) *Factory {
	session, err := gocql.NewSession(cfg, r, logger)
	if err != nil {
		logger.Fatal("unable to initialize cassandra session", tag.Error(err))
	}
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
		session:     session,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return newTaskPersistence(f.session, f.logger)
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return newShardPersistence(f.session, f.clusterName, f.logger)
}

// NewHistoryStore returns a new history store
func (f *Factory) NewHistoryStore() (p.HistoryStore, error) {
	return newHistoryPersistence(f.session, f.logger)
}

// NewMetadataStore returns a metadata store that understands only v2
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return newMetadataPersistence(f.session, f.clusterName, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return newClusterMetadataInstance(f.session, f.logger)
}

// NewExecutionStore returns an ExecutionStore for a given shardID
func (f *Factory) NewExecutionStore(shardID int32) (p.ExecutionStore, error) {
	factory, err := f.executionStoreFactory()
	if err != nil {
		return nil, err
	}
	return factory.new(shardID)
}

// NewVisibilityStore returns a visibility store
func (f *Factory) NewVisibilityStore() (p.VisibilityStore, error) {
	return newVisibilityPersistence(f.session, f.logger)
}

// NewQueue returns a new queue backed by cassandra
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return newQueue(f.session, f.logger, queueType)
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	if f.execStoreFactory != nil {
		f.execStoreFactory.close()
	}
}

func (f *Factory) executionStoreFactory() (*executionStoreFactory, error) {
	f.RLock()
	if f.execStoreFactory != nil {
		f.RUnlock()
		return f.execStoreFactory, nil
	}
	f.RUnlock()

	f.Lock()
	defer f.Unlock()

	if f.execStoreFactory != nil {
		return f.execStoreFactory, nil
	}

	factory, err := newExecutionStoreFactory(f.session, f.logger)
	if err != nil {
		return nil, err
	}

	f.execStoreFactory = factory
	return f.execStoreFactory, nil
}

// newExecutionStoreFactory is used to create an instance of ExecutionStoreFactory implementation
func newExecutionStoreFactory(
	session gocql.Session,
	logger log.Logger,
) (*executionStoreFactory, error) {
	return &executionStoreFactory{
		session: session,
		logger:  logger,
	}, nil
}

func (f *executionStoreFactory) close() {
	f.session.Close()
}

// new implements ExecutionStoreFactory interface
func (f *executionStoreFactory) new(
	shardID int32,
) (p.ExecutionStore, error) {
	pmgr, err := NewWorkflowExecutionPersistence(shardID, f.session, f.logger)
	if err != nil {
		return nil, err
	}
	return pmgr, nil
}
