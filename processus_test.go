package main

import "testing"

func TestParserValidJSON(t *testing.T) {
	in := make(chan Message, 5)
	out := make(chan Message, 5)

	go Parser(in, out)

	// Notification de processus
	in <- Message{Status: StatusNew, Message: "emitter"}

	// Message JSON valide
	in <- Message{
		MessageID:    "msg-test-01",
		Destinataire: "parser",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville":"Paris", "population":2102650}`,
	}

	res := <-out
	if res.MessageID != "msg-test-01" || res.Status != StatusOK {
		t.Fatalf("Parser attendait StatusOK pour JSON valide, reçu: %+v", res)
	}

	// Terminer le parser
	in <- Message{Status: StatusKill}
}

func TestParserInvalidJSON(t *testing.T) {
	in := make(chan Message, 5)
	out := make(chan Message, 5)

	go Parser(in, out)

	// Message JSON invalide
	in <- Message{
		MessageID:    "msg-test-02",
		Destinataire: "parser",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville": "Paris"`,
	}

	res := <-out
	if res.MessageID != "msg-test-02" || res.Status != StatusError {
		t.Fatalf("Parser attendait StatusError pour JSON invalide, reçu: %+v", res)
	}

	// Terminer le parser
	in <- Message{Status: StatusKill}
}

func TestEmitterProcess(t *testing.T) {
	in := make(chan Message, 5)
	out := make(chan Message, 5)

	go Emitter(in, out)

	// Inscription de parser
	in <- Message{Status: StatusNew, Message: "parser"}

	// Envoi d'un ACK simulé à l'emitter pour un message msg-001
	in <- Message{MessageID: "msg-001", Status: StatusOK}

	// Signal Kill
	in <- Message{Status: StatusKill}
}

func TestKillerProcess(t *testing.T) {
	in := make(chan Message, 5)
	out := make(chan Message, 5)

	go Killer(in, out)

	// Inscription simulée des processus
	in <- Message{Status: StatusNew, Message: "emitter"}

	// Arrêt propre avec kill
	in <- Message{Status: StatusKill}
}
