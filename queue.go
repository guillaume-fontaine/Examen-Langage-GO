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
