package storage

import "github.com/bsv-blockchain/go-sdk/overlay"

func OutputTopicKey(outpoint *overlay.Outpoint, topic string) string {
	return "ot:" + outpoint.String() + ":" + topic
}

// func SpendTopicKey(topic string) string {
// 	return "sp:" + topic
// }

const SpendsKey = "spends"

func outputKey(outpoint *overlay.Outpoint) string {
	return "o:" + outpoint.String()
}

var BeefKey = "beef"

func OutMembershipKey(topic string) string {
	return "om:" + topic
}

func TxMembershipKey(topic string) string {
	return "tm:" + topic
}
