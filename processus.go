package main

import (
	"encoding/json"
	"fmt"
)

// Parser écoute les messages sur son channel in, valide leur format JSON
// et renvoie la réponse (StatusOK ou StatusError) sur le channel out.
func Parser(in <-chan Message, out chan<- Message) {
	knownProcesses := make(map[string]bool)
	fmt.Println("[PARSER] Démarrage du processus parser")

	for msg := range in {
		switch msg.Status {
		case StatusNew:
			knownProcesses[msg.Message] = true
			fmt.Printf("[PARSER] Processus détecté : %s\n", msg.Message)

		case StatusPut:
			fmt.Printf("[PARSER] Message reçu : %s\n", msg.MessageID)

			var data map[string]interface{}
			err := json.Unmarshal([]byte(msg.Message), &data)

			if err != nil {
				fmt.Printf("[PARSER] Erreur de parsing JSON pour %s : %v\n", msg.MessageID, err)
				out <- Message{
					MessageID:    msg.MessageID,
					Destinataire: "",
					Status:       StatusError,
					Message:      err.Error(),
				}
			} else {
				fmt.Printf("[PARSER] JSON valide :\n%+v\n", data)
				fmt.Printf("[PARSER] Envoi ACK %s\n", msg.MessageID)
				out <- Message{
					MessageID: msg.MessageID,
					Status:    StatusOK,
				}
			}

		case StatusKill:
			fmt.Println("[PARSER] Signal kill reçu, arrêt du processus")
			return
		}
	}
}
