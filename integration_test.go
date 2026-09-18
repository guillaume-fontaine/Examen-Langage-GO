package main

import (
	"sync"
	"testing"
	"time"
)

// TestFullIntegrationScenario valide le flux nominal complet (Étape 10)
func TestFullIntegrationScenario(t *testing.T) {
	queue := NewQueue()

	emitterIn := make(chan Message, 10)
	parserIn := make(chan Message, 10)
	killerIn := make(chan Message, 10)

	go queue.Run()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		Emitter(emitterIn, queue.In)
	}()

	go func() {
		defer wg.Done()
		Parser(parserIn, queue.In)
	}()

	go func() {
		defer wg.Done()
		Killer(killerIn, queue.In)
	}()

	if !queue.AddProcess("emitter", emitterIn) {
		t.Errorf("échec d'enregistrement de emitter")
	}
	if !queue.AddProcess("parser", parserIn) {
		t.Errorf("échec d'enregistrement de parser")
	}
	if !queue.AddProcess("killer", killerIn) {
		t.Errorf("échec d'enregistrement de killer")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Arrêt propre confirmé
	case <-time.After(20 * time.Second):
		t.Fatalf("Le test d'intégration a dépassé le délai de 20s (blocage potentiel)")
	}
}

// TestErrorScenariosCoverage valide l'ensemble des cas d'erreur demandés (Étape 11)
func TestErrorScenariosCoverage(t *testing.T) {
	// 1. Processus déjà existant (refus)
	q := NewQueue()
	ch1 := make(chan Message, 5)
	ch2 := make(chan Message, 5)

	if !q.AddProcess("parser", ch1) {
		t.Errorf("Le premier enregistrement de parser aurait dû réussir")
	}
	if q.AddProcess("parser", ch2) {
		t.Errorf("Le deuxième enregistrement de parser aurait dû être refusé")
	}

	// 2. Destinataire inexistant
	emitterCh := make(chan Message, 5)
	q.AddProcess("emitter", emitterCh)
	<-emitterCh // consommer la notification 'new'

	go q.Run()

	q.In <- Message{
		MessageID:    "msg-err-01",
		Destinataire: "unknown",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      "test",
	}

	errMsg := <-emitterCh
	if errMsg.Status != StatusError {
		t.Errorf("Attendait StatusError pour destinataire inexistant, reçu: %+v", errMsg)
	}

	// 3. JSON invalide vers Parser
	pIn := make(chan Message, 5)
	pOut := make(chan Message, 5)
	go Parser(pIn, pOut)

	pIn <- Message{
		MessageID:    "msg-err-02",
		Destinataire: "parser",
		Emetteur:     "emitter",
		Status:       StatusPut,
		Message:      `{"ville": "Paris"`, // JSON incomplet
	}

	errParserRes := <-pOut
	if errParserRes.Status != StatusError || errParserRes.Destinataire != "" {
		t.Errorf("Parser aurait dû renvoyer StatusError avec Destinataire=\"\", reçu: %+v", errParserRes)
	}

	// 4. Signal Kill provoquant l'arrêt propre
	pIn <- Message{Status: StatusKill}
}
