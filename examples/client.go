package main

import (
	"amqprpc/amqprpc"
	"encoding/json"
	"os"

	log "github.com/sirupsen/logrus"
)


type Args struct {
	A int `msgpack:"a"`
	B int `msgpack:"b"`
}

type Result struct {
	Result int `msgpack:"result"`
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

func main() {
	client, err := amqprpc.NewClient(&amqprpc.Config{
		Dsn:               "amqp://admin:admin@localhost:5672/api-mq",
		ClientTimeout:     10,
		ReconnectInterval: 5,
		Log:               log.StandardLogger(),
		Exchange: 			amqprpc.SeamExchange,
		PrefetchCount:   1,
		Serializer: new(JsonSerializer),
	})

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100000; i++ {
		params := Args{A: 5, B: i}

		var result Result
		if err := client.Call(amqprpc.Six007rpcApi, params, &result); err != nil {
			log.Fatal(err)
		}
		log.Println(result)
	}

	if err := client.Close(); err != nil {
		log.Fatal(err)
	}
}


type JsonSerializer struct {}
func (s *JsonSerializer)Marshal(v interface{})([]byte, error) {
	return json.Marshal(v)
}
func (s *JsonSerializer)Unmarshal(data []byte, v interface{})  error{
	return json.Unmarshal(data, v)
}
func (s *JsonSerializer)GetContentType() string {
	return "application/json"
}

type JsonMethod struct {
	serializer  amqprpc.Serializer
}

func (m *JsonMethod) GetName() string {
	return "json"
}

func (m *JsonMethod) Setup(serializer amqprpc.Serializer) error {
	m.serializer = serializer
	return nil
}

func (m *JsonMethod) Cleanup() error {
	return nil
}

func (m *JsonMethod) Call(body []byte) (interface{}, *amqprpc.RPCError) {
	var params Args
	if err := m.serializer.Unmarshal(body, &params); err != nil {
		return nil, &amqprpc.RPCError{
			Err: amqprpc.ErrorData{
				Type:    "UnmarshalError",
				Message: err.Error(),
			},
		}
	}
	res := params.A * params.B
	log.Printf("Result: %d", res)
	return &Result{Result: res}, nil
}
