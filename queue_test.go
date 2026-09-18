package main

import "testing"

func TestAddProcess(t *testing.T) {
	q := NewQueue()

	emitterCh := make(chan Message, 10)
	parserCh := make(chan Message, 10)

	// 1. Ajouter emitter
	if !q.AddProcess("emitter", emitterCh) {
		t.Fatalf("échec enregistrement emitter")
	}

	// 2. Tenter de ré-ajouter emitter (refus attendu)
	if q.AddProcess("emitter", emitterCh) {
		t.Fatalf("l'enregistrement en double d'emitter aurait dû être refusé")
	}

	// 3. Ajouter parser
	if !q.AddProcess("parser", parserCh) {
		t.Fatalf("échec enregistrement parser")
	}

	// Vérifier la notification reçue par parser (doit recevoir emitter)
	msgParser := <-parserCh
	if msgParser.Status != StatusNew || msgParser.Message != "emitter" {
		t.Errorf("parser attendait notification emitter, reçu: %+v", msgParser)
	}

	// Vérifier la notification reçue par emitter (doit recevoir parser)
	msgEmitter := <-emitterCh
	if msgEmitter.Status != StatusNew || msgEmitter.Message != "parser" {
		t.Errorf("emitter attendait notification parser, reçu: %+v", msgEmitter)
	}
}

func TestRoutingPut(t *testing.T) {
	q := NewQueue()

	emitterCh := make(chan Message, 10)
	parserCh := make(chan Message, 10)

	q.AddProcess("emitter", emitterCh)
	q.AddProcess("parser", parserCh)

	// Consommer les notifications 'new'
	<-emitterCh
	<-parserCh

	go q.Run()

	// Case 1: emitter -> parser (destinataire existe)
	msg1 := Message{
		MessageID:    "msg-001",
		Destinataire: "parser",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville":"Paris"}`,
	}
	q.In <- msg1

	receivedByParser := <-parserCh
	if receivedByParser.MessageID != "msg-001" || receivedByParser.Destinataire != "parser" {
		t.Errorf("parser a reçu un message incorrect: %+v", receivedByParser)
	}

	// Case 2: emitter -> unknown (destinataire inexistant)
	msg2 := Message{
		MessageID:    "msg-002",
		Destinataire: "unknown",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville":"Berlin"}`,
	}
	q.In <- msg2

	errReceivedByEmitter := <-emitterCh
	if errReceivedByEmitter.Status != StatusError || errReceivedByEmitter.MessageID != "msg-002" {
		t.Errorf("emitter attendait une erreur destinataire inexistant, reçu: %+v", errReceivedByEmitter)
	}
}

func TestCorrelationResponse(t *testing.T) {
	q := NewQueue()

	emitterCh := make(chan Message, 10)
	parserCh := make(chan Message, 10)

	q.AddProcess("emitter", emitterCh)
	q.AddProcess("parser", parserCh)

	// Consommer les notifications 'new'
	<-emitterCh
	<-parserCh

	go q.Run()

	// 1. Emitter envoie un message put vers parser
	q.In <- Message{
		MessageID:    "msg-100",
		Destinataire: "parser",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville":"Lyon"}`,
	}

	// Parser reçoit le message
	msgFromQueue := <-parserCh

	// 2. Parser répond OK
	q.In <- Message{
		MessageID: msgFromQueue.MessageID,
		Status:    StatusOK,
		Message:   "parsing réussi",
	}

	// 3. Emitter doit recevoir la réponse de confirmation
	ackReceived := <-emitterCh
	if ackReceived.MessageID != "msg-100" || ackReceived.Status != StatusOK {
		t.Errorf("emitter n'a pas reçu le bon ACK : %+v", ackReceived)
	}

	// 4. La corrélation doit avoir été nettoyée dans la Queue
	if _, exists := q.messages["msg-100"]; exists {
		t.Errorf("la corrélation pour msg-100 aurait dû être supprimée")
	}
}
