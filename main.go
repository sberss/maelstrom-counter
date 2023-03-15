package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(node)
	ctx := context.Background()
	ch := make(chan int, 100)

	go func() {
		for {
			select {
			case delta := <-ch:
				v, err := kv.ReadInt(ctx, "counter")
				if err != nil {
					// Ignore if error is a key does not exist error.
					switch t := err.(type) {
					case *maelstrom.RPCError:
						if t.Code != maelstrom.KeyDoesNotExist {
							ch <- delta
							continue
						}
					default:
						ch <- delta
						continue
					}
				}

				if err := kv.CompareAndSwap(ctx, "counter", v, v+delta, true); err != nil {
					ch <- delta
					continue
				}
			}
		}
	}()

	node.Handle("add", func(msg maelstrom.Message) error {
		var reqBody map[string]any
		if err := json.Unmarshal(msg.Body, &reqBody); err != nil {
			return err
		}

		ch <- int(reqBody["delta"].(float64))

		// Ack.
		body := make(map[string]any)
		body["type"] = "add_ok"
		return node.Reply(msg, body)
	})

	node.Handle("read", func(msg maelstrom.Message) error {

		// Write a unique value to the KV store to ensure that the next read is the latest value.
		buf := make([]byte, 32)
		_, err := rand.Read(buf)
		if err != nil {
			return err
		}

		if err := kv.Write(ctx, fmt.Sprintf("%s-scratch", node.ID()), buf); err != nil {
			return err
		}

		v, err := kv.ReadInt(ctx, "counter")
		if err != nil {
			// Ignore if error is a key does not exist error.
			switch t := err.(type) {
			case *maelstrom.RPCError:
				if t.Code != maelstrom.KeyDoesNotExist {
					return err
				}
			default:
				return err
			}
		}

		// Ack.
		body := make(map[string]any)
		body["type"] = "read_ok"
		body["value"] = v
		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}

}
