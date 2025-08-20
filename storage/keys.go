package storage

import "github.com/bsv-blockchain/go-sdk/transaction"

func OutputTopicKey(outpoint *transaction.Outpoint, topic string) string {
	return "ot:" + topic + ":" + outpoint.String()
}

// func SpendTopicKey(topic string) string {
// 	return "sp:" + topic
// }

const SpendsKey = "spends"

func outputKey(outpoint *transaction.Outpoint) string {
	return "o:" + outpoint.String()
}

func OutMembershipKey(topic string) string {
	return "om:" + topic
}

func TxMembershipKey(topic string) string {
	return "tm:" + topic
}
