package crypto

import (
	"encoding/hex"
	"strings"

	"github.com/ecies/go/v2"
)

var EEInc = &encryptionEcies{}

type EEKeyPair struct {
	PrivateKey string `json:"private_key,omitempty"`
	PublicKey  string `json:"public_key,omitempty"`
}

type encryptionEcies struct{}

// Decrypt decrypts the message with the given private key
func (*encryptionEcies) Decrypt(hexValue, privateKey string) (message string, err error) {
	prv, err := eciesgo.NewPrivateKeyFromHex(privateKey)
	if err != nil {
		return
	}
	cipherBytes, err := hex.DecodeString(hexValue)
	if err != nil {
		return
	}
	plainBytes, err := eciesgo.Decrypt(prv, cipherBytes)
	if err != nil {
		return
	}
	message = string(plainBytes)
	return
}

// Encrypt encrypts the message with the given public key
func (*encryptionEcies) Encrypt(message, publicKey string) (hexValue string, err error) {
	pub, err := eciesgo.NewPublicKeyFromHex(publicKey)
	if err != nil {
		return
	}
	cipherBytes, err := eciesgo.Encrypt(pub, []byte(message))
	if err != nil {
		return
	}
	hexValue = hex.EncodeToString(cipherBytes)
	return
}

// GenerateKeyPair generates a new key pair
func (*encryptionEcies) GenerateKeyPair() (keyPair *EEKeyPair, err error) {
	k, err := eciesgo.GenerateKey()
	if err != nil {
		return
	}
	keyPair = &EEKeyPair{
		PrivateKey: strings.ToUpper(k.Hex()),
		PublicKey:  strings.ToUpper(k.PublicKey.Hex(true)),
	}
	return
}
