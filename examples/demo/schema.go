package demo

import (
	_ "embed"

	fomoxa "github.com/fomoxa/go"

	"github.com/fomoxa/frpc-go/examples/demo/generated"
)

//go:generate ../../tools/fomoxac.sh github.com/fomoxa/frpc-go/examples/demo

//go:embed .fomoxa/schema.json
var SchemaJSON []byte

func NetSchema() *fomoxa.Schema {
	messages := make([]fomoxa.Message, 0, len(generated.FomoxaMessages))
	for _, message := range generated.FomoxaMessages {
		messages = append(messages, fomoxa.Message{
			ID:          message.ID,
			Fingerprint: message.Fingerprint,
			Prefixes:    message.Prefixes,
		})
	}
	schema, err := fomoxa.NewSchema(generated.FomoxaSchemaFingerprint, messages)
	if err != nil {
		panic(err)
	}
	return schema
}
