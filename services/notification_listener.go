package services

import (
	"errors"
	"sync"

	"github.com/asdine/storm"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/smartcontractkit/chainlink/logger"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"go.uber.org/multierr"
)

// NotificationListener manages push notifications from the ethereum node's
// websocket to listen for new heads and log events.
type NotificationListener struct {
	Store             *store.Store
	HeadTracker       *HeadTracker
	subscriptions     []Subscription
	headNotifications chan models.BlockHeader
	headSubscription  *rpc.ClientSubscription
	subMutx           sync.Mutex
}

// Start obtains the jobs from the store and subscribes to logs and newHeads
// in order to start and resume jobs waiting on events or confirmations.
func (nl *NotificationListener) Start() error {
	nl.headNotifications = make(chan models.BlockHeader)

	ht, err := NewHeadTracker(nl.Store)
	if err != nil {
		return err
	}
	nl.HeadTracker = ht
	if err = nl.subscribeToNewHeads(); err != nil {
		return err
	}

	jobs, err := nl.Store.Jobs()
	if err != nil {
		return err
	}
	if err := nl.subscribeJobs(jobs); err != nil {
		return err
	}

	go nl.listenToNewHeads()
	return nil
}

// Stop gracefully closes its access to the store's EthNotifications.
func (nl *NotificationListener) Stop() error {
	if nl.headNotifications != nil {
		nl.headSubscription.Unsubscribe()
		close(nl.headNotifications)
		nl.unsubscribe() // have to iterate over all subscriptions and unsubscribe
	}
	return nil
}

func (nl *NotificationListener) subscribeJobs(jobs []models.Job) error {
	var err error
	for _, j := range jobs {
		err = multierr.Append(err, nl.AddJob(j))
	}
	return err
}

// AddJob looks for "runlog" and "ethlog" Initiators for a given job
// and watches the Ethereum blockchain for the addresses in the job.
func (nl *NotificationListener) AddJob(job models.Job) error {
	subscription, err := StartSubscription(job, nl.Store)
	if err != nil {
		return err
	}
	nl.addSubscription(sub)
	return nil
}

func (nl *NotificationListener) subscribeToNewHeads() error {
	sub, err := nl.Store.TxManager.SubscribeToNewHeads(nl.headNotifications)
	if err != nil {
		return err
	}
	nl.headSubscription = sub
	go func() {
		err := <-sub.Err()
		logger.Errorw("Error in new head subscription", "err", err)
	}()
	return nil
}

func (nl *NotificationListener) listenToNewHeads() {
	for head := range nl.headNotifications {
		if err := nl.HeadTracker.Save(&head); err != nil {
			logger.Error(err.Error())
		}
		pendingRuns, err := nl.Store.PendingJobRuns()
		if err != nil {
			logger.Error(err.Error())
		}
		for _, jr := range pendingRuns {
			if _, err := ExecuteRun(jr, nl.Store, models.RunResult{}); err != nil {
				logger.Error(err.Error())
			}
		}
	}
}

func (nl *NotificationListener) addSubscription(sub Subscription) {
	nl.subMutx.Lock()
	defer nl.subMutx.Unlock()
	nl.subscriptions = append(nl.subscriptions, sub)
}

func (nl *NotificationListener) unsubscribe() {
	nl.subMutx.Lock()
	defer nl.subMutx.Unlock()
	for _, sub := range nl.subscriptions {
		sub.Unsubscribe()
	}
}

// Holds and stores the latest block header experienced by this particular node
// in a thread safe manner. Reconstitutes the last block header from the data
// store on reboot.
type HeadTracker struct {
	store       *store.Store
	blockHeader *models.BlockHeader
	mutex       sync.Mutex
}

func (ht *HeadTracker) Save(bh *models.BlockHeader) error {
	if bh == nil {
		return errors.New("Cannot save a nil block header")
	}

	ht.mutex.Lock()
	if ht.blockHeader == nil || ht.blockHeader.Number.ToInt().Cmp(bh.Number.ToInt()) < 0 {
		copy := *bh
		ht.blockHeader = &copy
	}
	ht.mutex.Unlock()
	return ht.store.Save(bh)
}

func (ht *HeadTracker) Get() *models.BlockHeader {
	ht.mutex.Lock()
	defer ht.mutex.Unlock()
	return ht.blockHeader
}

// Instantiates a new HeadTracker using the store to persist
// new BlockHeaders
func NewHeadTracker(store *store.Store) (*HeadTracker, error) {
	ht := &HeadTracker{store: store}
	blockHeaders := []models.BlockHeader{}
	err := store.AllByIndex("Number", &blockHeaders, storm.Limit(1), storm.Reverse())
	if err != nil {
		return nil, err
	}
	if len(blockHeaders) > 0 {
		ht.blockHeader = &blockHeaders[0]
	}
	return ht, nil
}
