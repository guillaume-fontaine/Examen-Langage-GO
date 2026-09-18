package main

import "fmt"

const (
	StatusNew   = "new"
	StatusPut   = "put"
	StatusOK    = "ok"
	StatusError = "error"
	StatusKill  = "kill"
)

// Message représente les messages échangés dans la queue.
type Message struct {
	Message      string
	MessageID    string
	Destinataire string
	Emetteur     string
	Status       string
}

// Queue représente la file de messages centrale.
type Queue struct {
	processes map[string]chan Message
	messages  map[string]string
	In        chan Message
}

// NewQueue crée et initialise une nouvelle instance de Queue.
func NewQueue() *Queue {
	return &Queue{
		processes: make(map[string]chan Message),
		messages:  make(map[string]string),
		In:        make(chan Message),
	}
}

// AddProcess enregistre un nouveau processus dans la Queue.
// Si le processus existe déjà, la fonction retourne false.
// Sinon, elle enregistre le processus, notifie les processus existants
// et informe le nouveau des processus déjà présents.
func (q *Queue) AddProcess(id string, ch chan Message) bool {
	if _, exists := q.processes[id]; exists {
		fmt.Printf("[QUEUE] Refus d'enregistrement : le processus '%s' existe déjà\n", id)
		return false
	}

	// Notifier les anciens processus et informer le nouveau processus des existants
	for existingID, existingCh := range q.processes {
		existingCh <- Message{
			Status:  StatusNew,
			Message: id,
		}
		ch <- Message{
			Status:  StatusNew,
			Message: existingID,
		}
	}

	q.processes[id] = ch
	fmt.Printf("[QUEUE] Processus enregistré : %s\n", id)
	return true
}

// Run écoute continuellement le channel d'entrée In et effectue le routage des messages.
func (q *Queue) Run() {
	for msg := range q.In {
		switch msg.Status {
		case StatusPut:
			destCh, exists := q.processes[msg.Destinataire]
			if !exists {
				fmt.Printf("[QUEUE] Erreur : destinataire '%s' inexistant pour le message %s\n", msg.Destinataire, msg.MessageID)
				if senderCh, ok := q.processes[msg.Emetteur]; ok {
					senderCh <- Message{
						MessageID:    msg.MessageID,
						Destinataire: msg.Emetteur,
						Status:       StatusError,
						Message:      fmt.Sprintf("Destinataire inexistant : %s", msg.Destinataire),
					}
				}
			} else {
				q.messages[msg.MessageID] = msg.Emetteur
				fmt.Printf("[QUEUE] Routage %s vers %s\n", msg.MessageID, msg.Destinataire)
				destCh <- msg
			}
		}
	}
}
