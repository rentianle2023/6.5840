package main

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"log"
)

func main() {
	connection, err := ethclient.Dial("https://mainnet.infura.io/v3/f88c84f2b3b14be5bb4477b3eb680819")
	if err != nil {
		log.Fatal("Failed to connect to Eth Node", err)
	}

	account := common.HexToAddress("0x769bcC0925351EA67488Fa9ee0f56776b0a6C324")

	balance, err := connection.BalanceAt(context.Background(), account, nil)
	if err != nil {
		log.Fatal("Unable to get balance", err)
	}
	fmt.Println(balance)

	/* tx, pending, err := connection.TransactionByHash(cntxt, common.HexToHash("0xcd1363a0854f4bf5597630b61de58faaebe7742093564a2d82f6fe3d0a309d0e"))

	   if err != nil {
	      log.Fatal("Failed to get transaction by hash", err)
	   }

	   if pending {
	      fmt.Println("Transaction is pending")
	   } else {
	      fmt.Println("Transaction is not pending", tx)
	   }*/

	/* privateKey, err := crypto.GenerateKey()
	   if err != nil {
	      log.Fatal("unable to generate new keys")
	   }

	   privateKeyBytes := crypto.FromECDSA(privateKey)

	   fmt.Println(hexutil.Encode(privateKeyBytes))*/
}
