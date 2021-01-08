package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

//waitgroups to let the main function wait until all goroutines are finished
var wg = sync.WaitGroup{}

//Intially a leader is selected based on prior prioirty
var leader int = 3

//Activestates, zone name and priority of seceral zones
var activeStates [6]string
var zone [6]string
var priority [6]int

func main() {

	fmt.Println("Leader Election Algorithm in a Ring")
	//Intializing the prioirties based on several factors like distance, bandwidth etc.,
	priority[0] = 3
	priority[1] = 32
	priority[2] = 5
	priority[3] = 80
	priority[4] = 6
	priority[5] = 12
	//Intially all zones are active
	activeStates[0] = "Active"
	activeStates[1] = "Active"
	activeStates[2] = "Active"
	activeStates[3] = "Active"
	activeStates[4] = "Active"
	activeStates[5] = "Active"
	//Names of each zone
	zone[0] = "ohio"
	zone[1] = "virginia"
	zone[2] = "oregon"
	zone[3] = "Mumbai"
	zone[4] = "london"
	zone[5] = "milan"

	fmt.Println("List of all the DB Replicas named with Zones \n 1.Ohio \n 2.Virginia \n 3.Oregon \n 4.Mumbai \n 5.London \n 6.Milan")

	for true {
		fmt.Println("Options:-")
		fmt.Println("1.Process a peer-to-peer communication")
		fmt.Println("2.Fail the DB Replica")
		fmt.Println("3.Display the leader which is responsible for Write Reqest into DB")
		fmt.Println("4.Exit")

		var input int
		fmt.Scanln(&input)

		switch input {
		//To communicate with the leader to update the db logs
		case 1:
			fmt.Println("From which Instance you want to communicate with the leader (i.e.,To sync it's db logs)")
			var instanceName string
			fmt.Scanln(&instanceName) //Getting the name from user
			//Adding two wait groups
			wg.Add(2)
			transanction(instanceName)
			wg.Wait()

		//To intentially fail a zone to test the algorithm
		case 2:
			fmt.Println("Enter the Zone where DB Replica is Failed")
			var replica string
			fmt.Scanln(&replica)

			if replica == "ohio" {

				activeStates[0] = "Failed"

			} else if replica == "virginia" {

				activeStates[1] = "Failed"

			} else if replica == "oregon" {

				activeStates[2] = "Failed"

			} else if replica == "mumbai" {

				activeStates[3] = "Failed"

			} else if replica == "london" {

				activeStates[4] = "Failed"

			} else if replica == "milan" {

				activeStates[5] = "Failed"

			} else {

				fmt.Println("Your DB is not available in this Zone")

			}

		//Displaying the current leader
		case 3:
			fmt.Println("Your Leader is in Zone", zone[leader])

		case 4:
			os.Exit(3)

		}
		fmt.Println("------------------------------------------------")
	}
}

func ohio(ch chan int, origin int) {
	var id int = 0
	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if the zone leader and request is from another zone
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}
		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()
}

func virginia(ch chan int, origin int) {
	var id int = 1

	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if the zone leader and request is from another zone
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}

		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()

}

func oregon(ch chan int, origin int) {
	var id int = 2

	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if the zone leader and request is from another zone
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}
		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()
}

func mumbai(ch chan int, origin int) {
	var id int = 3
	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if the zone leader and request is from another zone
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}
		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()
}

func london(ch chan int, origin int) {
	var id int = 4
	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if the zone leader and request is from another zone
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}
		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()
}

func milan(ch chan int, origin int) {
	var id int = 5
	if origin == id && leader == id { //if the zone is leader and sent a requast to itself
		if activeStates[id] == "Active" {
			fmt.Println("Your Transaction is Fullfilled")
		} else {
			fmt.Println("Sorry your DB Instance in this Zone is not available")
		}
	} else if leader == id { //if i am leader and request is from another
		data := <-ch
		if activeStates[id] == "Active" { //check if leader is active
			if activeStates[origin] == "Active" { //check if the zone from which the request came is active
				fmt.Println("Your Request is Fullfilled which came from", zone[data])
			} else {
				fmt.Println("The DB Instance from where req is processed is Failed\n cannot Process the Request")
			}
		} else { //If the leader is not active(failed), then start an election
			electionAlgorithm(data)
		}
	} else { //Sending request to the leader to access db logs
		ch <- id
	}
	wg.Done()
}

func electionAlgorithm(detectedProcess int) {
	//The algrithm for election used here is ELECTION IN A RING
	count := 0
	//Intially the process which noticed the failure assumed to be the new leader
	newpriority := priority[detectedProcess]
	//The elecetion is passed around the ring until a loop is completed and leader is changed based on prioirty of each process
	for i := detectedProcess; count < 5; i = (i + 1) % 6 {
		if (newpriority < priority[(i+1)%6]) && activeStates[(i+1)%6] == "Active" {
			leader = (i + 1) % 6
			newpriority = priority[(i+1)%6]
		}
		// fmt.Println(leader)
		count = count + 1
	}

	fmt.Println("\n", zone[detectedProcess], " has detected that leader has fallen down so started Election algorithm ")
	time.Sleep(time.Second * 3)
	fmt.Println("\n Now the leader is in ", zone[leader], " Zone")

}

func transanction(from string) {
	ch := make(chan int)
	//Based on the selection of request sending zone, appropriate goroutines are invoked
	if from == "ohio" {
		go ohio(ch, 0)
		switch leader {
		case 0:
			go ohio(ch, 0)
		case 1:
			go virginia(ch, 0)
		case 2:
			go oregon(ch, 0)
		case 3:
			go mumbai(ch, 0)
		case 4:
			go london(ch, 0)
		case 5:
			go milan(ch, 0)
		}

	} else if from == "virginia" {
		go virginia(ch, 1)
		switch leader {
		case 0:
			go ohio(ch, 1)
		case 1:
			go virginia(ch, 1)
		case 2:
			go oregon(ch, 1)
		case 3:
			go mumbai(ch, 1)
		case 4:
			go london(ch, 1)
		case 5:
			go milan(ch, 1)
		}

	} else if from == "oregon" {

		go oregon(ch, 2)
		switch leader {
		case 0:
			go ohio(ch, 2)
		case 1:
			go virginia(ch, 2)
		case 2:
			go oregon(ch, 2)
		case 3:
			go mumbai(ch, 2)
		case 4:
			go london(ch, 2)
		case 5:
			go milan(ch, 2)
		}

	} else if from == "mumbai" {

		go mumbai(ch, 3)
		switch leader {
		case 0:
			go ohio(ch, 3)
		case 1:
			go virginia(ch, 3)
		case 2:
			go oregon(ch, 3)
		case 3:
			go mumbai(ch, 3)
		case 4:
			go london(ch, 3)
		case 5:
			go milan(ch, 3)
		}

	} else if from == "london" {

		go london(ch, 4)
		switch leader {
		case 0:
			ohio(ch, 4)
		case 1:
			go virginia(ch, 4)
		case 2:
			go oregon(ch, 4)
		case 3:
			go mumbai(ch, 4)
		case 4:
			go london(ch, 4)
		case 5:
			go milan(ch, 4)
		}

	} else if from == "milan" {

		go milan(ch, 5)
		switch leader {
		case 0:
			go ohio(ch, 5)
		case 1:
			go virginia(ch, 5)
		case 2:
			go oregon(ch, 5)
		case 3:
			go mumbai(ch, 5)
		case 4:
			go london(ch, 5)
		case 5:
			go milan(ch, 5)
		}

	}

}
