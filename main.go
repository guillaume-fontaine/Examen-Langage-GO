package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== Démarrage du système Message Queue ===")

	queue := NewQueue()

	emitterIn := make(chan Message, 10)
	parserIn := make(chan Message, 10)
	killerIn := make(chan Message, 10)

	// Lancer la Queue dans une goroutine
	go queue.Run()

	var wg sync.WaitGroup
	wg.Add(3)

	// Lancer les 3 processus concurrents
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

	// Enregistrer les processus auprès de la Queue
	queue.AddProcess("emitter", emitterIn)
	queue.AddProcess("parser", parserIn)
	queue.AddProcess("killer", killerIn)

	// Attendre que le signal kill donné par le Killer provoque l'arrêt des goroutines
	wg.Wait()

	time.Sleep(100 * time.Millisecond)
	fmt.Println("=== Arrêt complet du système Message Queue ===")
}
