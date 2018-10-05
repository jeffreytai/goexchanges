package main

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"strings"

	"github.com/gorilla/websocket"
)

const (
	WebsocketAddress = "wss://api.gemini.com/v1/marketdata/"
	ExchangeName     = "Gemini"
)

var (
	//supportedInstruments = []string{"BTCUSD", "ETHUSD", "ZECUSD"}
	supportedInstruments = []string{"BTCUSD"}

	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}

	logger, _ = zap.NewDevelopment()
	slogger   = logger.Sugar()
)

func main() {
	initialize()
}

func initialize() {
	fmt.Println("Connecting to Gemini")

	for _, instrument := range supportedInstruments {
		sb := strings.Builder{}
		sb.WriteString(WebsocketAddress)
		sb.WriteString(instrument)
		// TODO: In the future change the heartbeat to true
		sb.WriteString("?heartbeat=false")

		subscribe(sb.String())
	}
}

type Event struct {
	Type      string  `json:"type"`
	Reason    string  `json:"reason"`
	Price     float32 `json:"price"`
	Delta     float32 `json:"delta"`
	Remaining float32 `json:"remaining"`
	Side      string  `json:"side"`
}

type InitialState struct {
	Type           string  `json:"type"`
	EventId        uint64  `json:"eventId"`
	SocketSequence uint32  `json:"socket_sequence"`
	Events         []Event `json:"events"`
}

type Update struct {
	Type           string  `json:"type"`
	EventId        uint64  `json:"eventId"`
	Timestamp      uint64  `json:"timestamp"`
	Timestampms    uint64  `json:"timestampms"`
	SocketSequence uint32  `json:"socket_sequence"`
	Events         []Event `json:"events"`
}

func subscribe(address string) {
	con, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		slogger.Errorf("[%s] Error connecting to websocket", ExchangeName)
		return
	}
	defer con.Close()

	// The initial response message will show the existing state of the order book. Subsequent messages will show all
	// executed trades, as well as all other changes to the order book from orders placed or canceled.
	_, initialState, err := con.ReadMessage()
	if err != nil {
		slogger.Errorf("[%s] Error retrieving initial state of order book", ExchangeName)
	}
	processInitialState(initialState)

	for {
		_, update, err := con.ReadMessage()
		if err != nil {
			slogger.Errorf("[%s] Error in reading message %s", ExchangeName, err.Error())
		}
		processUpdate(update)
	}
}

func processInitialState(bytes []byte) {
	var state InitialState
	json.Unmarshal(bytes, &state)

	slogger.Infof("[%s] Parsed: %+v\n", ExchangeName, state)
}

func processUpdate(bytes []byte) {
	var update Update
	json.Unmarshal(bytes, &update)

	slogger.Infof("[%s] Parsed: %+v\n", ExchangeName, update)
}
