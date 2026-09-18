package main

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
	Status       string
}
