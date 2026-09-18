package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Exemples de données JSON (villes européennes) pour l'Emitter
var sampleCities = []string{
	`{"ville": "Paris", "pays": "France", "population": 2102650, "capitale": true}`,
	`{"ville": "Berlin", "pays": "Allemagne", "population": 3645000, "capitale": true}`,
	`{"ville": "Madrid", "pays": "Espagne", "population": 3223000, "capitale": true}`,
	`{"ville": "Rome", "pays": "Italie", "population": 2873000, "capitale": true}`,
	`{"ville": "Amsterdam", "pays": "Pays-Bas", "population": 821750, "capitale": true}`,
}

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

// Emitter envoie périodiquement des messages JSON au processus parser.
func Emitter(in <-chan Message, out chan<- Message) {
	knownProcesses := make(map[string]bool)
	pendingMessages := make(map[string]string)
	fmt.Println("[EMITTER] Démarrage du processus emitter")

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	msgCounter := 0
	cityIndex := 0

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			switch msg.Status {
			case StatusNew:
				knownProcesses[msg.Message] = true
				fmt.Printf("[EMITTER] Processus détecté : %s\n", msg.Message)

			case StatusOK:
				fmt.Printf("[EMITTER] Message %s traité avec succès (ACK ok)\n", msg.MessageID)
				delete(pendingMessages, msg.MessageID)

			case StatusError:
				fmt.Printf("[EMITTER] Erreur sur le message %s : %s\n", msg.MessageID, msg.Message)
				delete(pendingMessages, msg.MessageID)

			case StatusKill:
				fmt.Println("[EMITTER] Signal kill reçu, arrêt du processus")
				return
			}

		case <-ticker.C:
			if !knownProcesses["parser"] {
				fmt.Println("[EMITTER] En attente de la disponibilité du parser...")
				continue
			}

			msgCounter++
			msgID := fmt.Sprintf("msg-%03d", msgCounter)
			jsonContent := sampleCities[cityIndex%len(sampleCities)]
			cityIndex++

			pendingMessages[msgID] = jsonContent

			fmt.Printf("[EMITTER] Envoi du message %s vers parser\n", msgID)
			out <- Message{
				MessageID:    msgID,
				Destinataire: "parser",
				Emetteur:     "emitter",
				Status:       StatusPut,
				Message:      jsonContent,
			}
		}
	}
}
