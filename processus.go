package main

import (
	"encoding/json"
	"fmt"
	"time"
)

// Exemples de données JSON (villes européennes) pour l'Emitter
var sampleCities = []string{
	`{"ville": "Paris", "pays": "France", "population": 2102650, "langue": "français", "monnaie": "EUR", "latitude": 48.8566, "longitude": 2.3522, "capitale": true}`,
	`{"ville": "Berlin", "pays": "Allemagne", "population": 3700000, "langue": "allemand", "monnaie": "EUR", "latitude": 52.5200, "longitude": 13.4050, "capitale": true}`,
	`{"ville": "Madrid", "pays": "Espagne", "population": 3400000, "langue": "espagnol", "monnaie": "EUR", "latitude": 40.4168, "longitude": -3.7038, "capitale": true}`,
	`{"ville": "Rome", "pays": "Italie", "population": 2800000, "langue": "italien", "monnaie": "EUR", "latitude": 41.9028, "longitude": 12.4964, "capitale": true}`,
	`{"ville": "Lisbonne", "pays": "Portugal", "population": 550000, "langue": "portugais", "monnaie": "EUR", "latitude": 38.7223, "longitude": -9.1393, "capitale": true}`,
	`{"ville": "Amsterdam", "pays": "Pays-Bas", "population": 930000, "langue": "néerlandais", "monnaie": "EUR", "latitude": 52.3676, "longitude": 4.9041, "capitale": true}`,
	`{"ville": "Vienne", "pays": "Autriche", "population": 2000000, "langue": "allemand", "monnaie": "EUR", "latitude": 48.2082, "longitude": 16.3738, "capitale": true}`,
	`{"ville": "Prague", "pays": "République tchèque", "population": 1400000, "langue": "tchèque", "monnaie": "CZK", "latitude": 50.0755, "longitude": 14.4378, "capitale": true}`,
	`{"ville": "Stockholm", "pays": "Suède", "population": 1000000, "langue": "suédois", "monnaie": "SEK", "latitude": 59.3293, "longitude": 18.0686, "capitale": true}`,
	`{"ville": "Athènes", "pays": "Grèce", "population": 650000, "langue": "grec", "monnaie": "EUR", "latitude": 37.9838, "longitude": 23.7275, "capitale": true}`,
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

// Killer attend 15 secondes puis transmet un signal kill à l'ensemble des processus connus.
func Killer(in <-chan Message, out chan<- Message) {
	knownProcesses := make(map[string]bool)
	fmt.Println("[KILLER] Démarrage du processus killer")

	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	for {
		select {
		case msg, ok := <-in:
			if !ok {
				return
			}
			switch msg.Status {
			case StatusNew:
				knownProcesses[msg.Message] = true
				fmt.Printf("[KILLER] Processus détecté : %s\n", msg.Message)
			case StatusKill:
				fmt.Println("[KILLER] Signal kill reçu, arrêt du killer")
				return
			}

		case <-timer.C:
			fmt.Println("[KILLER] Envoi du signal kill à tous les processus")
			for target := range knownProcesses {
				out <- Message{
					Destinataire: target,
					Status:       StatusKill,
				}
			}
			fmt.Println("[KILLER] Signal kill envoyé. Arrêt du killer.")
			return
		}
	}
}
