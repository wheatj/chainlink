package services

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/smartcontractkit/chainlink/logger"
	"github.com/smartcontractkit/chainlink/store"
	"github.com/smartcontractkit/chainlink/store/models"
	"github.com/smartcontractkit/chainlink/store/presenters"
	"github.com/smartcontractkit/chainlink/utils"
	"go.uber.org/multierr"
)

const (
	EventTopicSignature = iota
	EventTopicRequestID
	EventTopicJobID
)

// RunLogTopic is the signature for the Request(uint256,bytes32,string) event
// which Chainlink RunLog initiators watch for.
// See https://github.com/smartcontractkit/chainlink/blob/master/solidity/contracts/Oracle.sol
var RunLogTopic = common.HexToHash("0x06f4bf36b4e011a5c499cef1113c2d166800ce4013f6c2509cab1a0e92b83fb2")

type Subscription struct {
	Job              models.Job
	store            *store.Store
	logNotifications chan types.Log
	errors           chan error
	rpcSubscription  *rpc.ClientSubscription
}

func StartSubscription(job models.Job, store *store.Store) (Subscription, error) {
	sub := Subscription{Job: job, store: store}
	sub.errors = make(chan error)
	sub.logNotifications = make(chan types.Log)

	var addresses []common.Address
	for _, initr := range job.InitiatorsFor(models.InitiatorEthLog, models.InitiatorRunLog) {
		msg := fmt.Sprintf("Listening for logs from address %v", presenters.LogListeningAddress(initr.Address))
		logger.Debugw(msg)
		addresses = append(addresses, initr.Address)
	}

	if len(addresses) == 0 {
		return sub, nil
	}

	rpc, err := nl.Store.TxManager.SubscribeToLogs(nl.logNotifications, nl.filterQueryFor(addresses))
	if err != nil {
		return sub, err
	}
	sub.rpcSubscription = rpc
	//go nl.listenToSubscriptionErrors()
	//go nl.listenToLogs()
	return sub, nil
}

func (nl *NotificationListener) filterQueryFor(addresses []common.Address) ethereum.FilterQuery {
	blockHeader := nl.HeadTracker.Get()
	var fromBlock *big.Int
	if blockHeader != nil {
		fromBlock = blockHeader.Number.ToInt()
	}
	return ethereum.FilterQuery{
		FromBlock: fromBlock,
		Addresses: utils.WithoutZeroAddresses(addresses), // this is actually wrong because zero addresses should result in listening to all AND those restricted
	}
}

func (sub Subscription) Unsubscribe() {
	if rpcSubscription != nil {
		rpcSubscription.Unsubscribe()
	}
	//close(nl.errors)
	//close(nl.logNotifications)
}

func (nl *NotificationListener) listenToSubscriptionErrors() {
	for err := range nl.errors {
		logger.Errorw("Error in log subscription", "err", err)
	}
}

func (nl *NotificationListener) listenToLogs() {
	for el := range nl.logNotifications {
		nl.receiveLog(el)
	}
}

func (nl *NotificationListener) receiveLog(el types.Log) {
	msg := fmt.Sprintf("Received log from %v", el.Address.String())
	logger.Debugw(msg, "log", el)

	initrs, err := InitiatorsForLog(nl.Store, el)
	if err != nil {
		logger.Errorw(err.Error())
		return
	}

	for _, initr := range initrs {
		job, err := nl.Store.FindJob(initr.JobID)
		if err != nil {
			logger.Errorw(fmt.Sprintf("Error initiating job from log: %v", err),
				"job", initr.JobID, "initiator", initr.ID)
			continue
		}

		input, err := FormatLogJSON(initr, el)
		if err != nil {
			logger.Errorw(err.Error(), "job", initr.JobID, "initiator", initr.ID)
			continue
		}

		if _, err = BeginRun(job, nl.Store, input); err != nil {
			logger.Errorw(err.Error(), "job", initr.JobID, "initiator", initr.ID)
		}
	}
}

// FormatLogJSON uses the Initiator to decide how to format the EventLog
// as a JSON object.
func FormatLogJSON(initr models.Initiator, el types.Log) (models.JSON, error) {
	if initr.Type == models.InitiatorEthLog {
		return ethLogJSON(el)
	} else if initr.Type == models.InitiatorRunLog {
		out, err := runLogJSON(el)
		return out, err
	}
	return models.JSON{}, fmt.Errorf("no supported initiator type was found")
}

func ethLogJSON(el types.Log) (models.JSON, error) {
	var out models.JSON
	b, err := json.Marshal(el)
	if err != nil {
		return out, err
	}
	return out, json.Unmarshal(b, &out)
}

func runLogJSON(el types.Log) (models.JSON, error) {
	js, err := decodeABIToJSON(el.Data)
	if err != nil {
		return js, err
	}

	js, err = js.Add("address", el.Address.String())
	if err != nil {
		return js, err
	}

	js, err = js.Add("dataPrefix", el.Topics[EventTopicRequestID].String())
	if err != nil {
		return js, err
	}

	return js.Add("functionSelector", "76005c26")
}

func decodeABIToJSON(data hexutil.Bytes) (models.JSON, error) {
	varLocationSize := 32
	varLengthSize := 32
	var js models.JSON
	hex := []byte(string([]byte(data)[varLocationSize+varLengthSize:]))
	return js, json.Unmarshal(bytes.TrimRight(hex, "\x00"), &js)
}

// InitiatorsForLog returns all of the Initiators relevant to a log.
func InitiatorsForLog(store *store.Store, log types.Log) ([]models.Initiator, error) {
	initrs, merr := ethLogInitrsForAddress(store, log.Address)
	if isRunLog(log) {
		rlInitrs, err := runLogInitrsForLog(store, log)
		initrs = append(initrs, rlInitrs...)
		merr = multierr.Append(merr, err)
	}

	return initrs, merr
}

func ethLogInitrsForAddress(store *store.Store, address common.Address) ([]models.Initiator, error) {
	query := store.Select(q.And(q.Or(q.Eq("Address", address), q.Eq("Address", utils.ZeroAddress)), q.Re("Type", models.InitiatorEthLog)))
	initrs := []models.Initiator{}
	return initrs, allowNotFoundError(query.Find(&initrs))
}

func runLogInitrsForLog(store *store.Store, log types.Log) ([]models.Initiator, error) {
	initrs := []models.Initiator{}
	if !isRunLog(log) {
		return initrs, nil
	}
	jobID, err := jobIDFromLog(log)
	if err != nil {
		return initrs, err
	}

	query := store.Select(q.And(q.Eq("JobID", jobID), q.Re("Type", models.InitiatorRunLog)))
	if err = query.Find(&initrs); allowNotFoundError(err) != nil {
		return initrs, err
	}
	return initrsForAddress(initrs, log.Address), nil
}

func allowNotFoundError(err error) error {
	if err == storm.ErrNotFound {
		return nil
	}
	return err
}

func isRunLog(log types.Log) bool {
	return len(log.Topics) == 3 && log.Topics[0] == RunLogTopic
}

func jobIDFromLog(log types.Log) (string, error) {
	return utils.HexToString(log.Topics[EventTopicJobID].Hex())
}

func initrsForAddress(initrs []models.Initiator, addr common.Address) []models.Initiator {
	good := []models.Initiator{}
	for _, initr := range initrs {
		if utils.IsEmptyAddress(initr.Address) || initr.Address == addr {
			good = append(good, initr)
		}
	}
	return good
}