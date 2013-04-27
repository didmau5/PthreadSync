/*
Daniel Mow
MFS
Last Edited: March 5th, 2013

README:
This is a Multi-Threaded Flow Schedualler (MFS) simulation that
uses a schedualler thread and a thread to represent each flow.
It uses 3 mutexes: one to protect the number of flows, one to protect the 
transmission function, and one to protect the waiting queue.
Two condtition variables are used: one to wake all waiting threads to check if it
is their turn to transmit, and one to begin the schedualler from main and after
a flow has transmitted.

*/

#include <pthread.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#define MAX_INPUT_LINELEN 50


pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_trans = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_q = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_cond_t trans_cond = PTHREAD_COND_INITIALIZER;




typedef struct Flow{
	int id;
	int arrival;
	int trans;
	int priority;
}Flow;
void printFlow(Flow *flow);

typedef struct Node{
	Flow data;
	struct Node *next;
}Node;

typedef struct Queue{
	Node *head;
	Node *tail;
	int size;
}Queue;
void enQ(Flow item);
Node deQ();
void sort();


/*thread functions*/
void *thread_func(void* arg);
void *sched_thread_func(void *arg);

/*global vars */
int numFlows;
Flow *flows;
Queue remFlows;
int transID;

/*Initialize start time */
struct timeval t0,t1,t2;
float timer;

/* This function simulates the transmission of a flow */
void
transmit(Flow flow){
	
	gettimeofday(&t1, NULL);
	timer = (t1.tv_sec - t0.tv_sec);   
	timer += (t1.tv_usec - t0.tv_usec)/1000000.0;
	printf("Flow %d starts its transmission at time %.2f\n", flow.id, timer);
	sleep(flow.trans/10);
	gettimeofday(&t1, NULL);
	timer = ( t1.tv_sec - t0.tv_sec );
	timer += (t1.tv_usec - t0.tv_usec)/1000000.0;
	printf("Flow %d finishes its transmission at time %.2f\n", flow.id, timer);
	
}

/* This function initializes an array of flows based on the info in input */
void
initFlows(FILE *input, Flow *array){


	char buffer[MAX_INPUT_LINELEN];
	char *lnptr;
	int intTok;
	int counter;
	int i;		/* flow counter */
	
	counter = 0; /* switch controller - 0:consuming id, 1:consuming arrival
					time, 2: consuming transmission time, 3:consuming priority*/
	i = 0;
	
	while(fscanf(input, "%s", &buffer) != EOF){
		lnptr = strtok(buffer, ":,");
		intTok = atoi(lnptr);
		while (lnptr != NULL){
			/* determine which flow attribute filled by counter */
			switch(counter){
				case 0:
					array[i].id = intTok;
					break;
				case 1:
					array[i].arrival = intTok;
					break;
				case 2:
					array[i].trans = intTok;
					break;
				case 3:
					array[i].priority = intTok;
					break;
			}
			counter ++;
			lnptr = strtok(NULL, ":,");
			if (lnptr != NULL){
				intTok = atoi(lnptr);
			}
		}
		counter = 0;
		i++;
	}
}



int
main(int argc, char* argv[]){

	
	pthread_t *tid;
	pthread_t sched;
	int i;
	//set t0
	gettimeofday(&t0, NULL);
	/* initialize queue */
	remFlows.head = NULL;
	remFlows.tail = NULL;
	remFlows.size = 0;

	printf("MFS Simulation Start...\n");
	
	/* CREATE SCHED THREAD */
	if (pthread_create(&sched, NULL, sched_thread_func, NULL) != 0){
		printf("SCHED thread fail");
		exit(1);
	}
	
	/* READ IN FILE*/
	FILE *input;
	
	input = fopen(argv[1], "r");
	
	if (input == NULL || argc != 2){
		printf("FILE OPEN ERROR\n");
		printf("usage: specify input file\n");
		exit(1);
	}
	
	else{
		pthread_mutex_lock( &mutex );
		/*consume NUMFLOWS*/
		fscanf(input, "%d", &numFlows);
		
		
		Flow tempFlows[numFlows];
		flows = tempFlows;
		initFlows(input,flows);
		
		/*define tid  */
		pthread_t thd_flows[numFlows];
		tid = thd_flows;
		
		/*CREATE FLOW THREADS*/
		
		for (i = 0; i < numFlows; i++){
			if (pthread_create(&thd_flows[i], NULL, thread_func, (void *)&flows[i]) != 0){
				printf("thread fail");
			}
			
		}
		pthread_mutex_unlock( &mutex );
		pthread_mutex_lock( &mutex_q);
		
		/* Signal sched thread  */
		while(numFlows > 0){
			pthread_cond_signal(&cond);
			pthread_mutex_unlock(&mutex_q);
		}
		
		
		/* JOIN THREADS  */
		for (i = 0; i < numFlows; i++){
			if (pthread_join(thd_flows[i], NULL) != 0){
				printf("thread join fail");
			}	
		}
		pthread_cond_destroy(&cond);
		pthread_cancel(sched);
		if (pthread_join(sched, NULL) != 0){
			printf("sched thread join fail");
		}

	}
	printf("MFS Simulation End\n");
	return (0);
}

/* Flow Thread Initial Function */
void *
thread_func(void * arg){

	struct Flow flow = *(Flow *) arg;

	/*	wait for arrival time (given in tenths of a second)	*/
	usleep(flow.arrival*100000);
	printf("Flow %d arrives: arrival time (%.2f), transmission time (%.1f), priority (%d)\n", flow.id, (float)(flow.arrival/10.0), (float)(flow.trans/10.0), flow.priority);
	
	pthread_mutex_lock(&mutex_q);
	/*  enQ to waiting flows  */
	enQ(flow);
	pthread_mutex_unlock(&mutex_q);
	
	
	
	pthread_mutex_lock(&mutex_trans);
	
	
	while(transID != flow.id){
		if(transID !=0){
			printf("Flow %d waits for the finish of flow %d\n", flow.id, transID);
		}
		pthread_cond_wait(&trans_cond, &mutex_trans);
		//sleep(1);
	}
	
	transmit(flow);

	pthread_mutex_unlock(&mutex_trans);
	pthread_mutex_lock(&mutex_q);
	
	remFlows.size--;
	numFlows--;
	
	while(remFlows.size > 0){
		/* Signal sched thread  */
		pthread_cond_signal(&cond);
		pthread_mutex_unlock(&mutex_q);
	}
	//pthread_cond_destroy(&cond);
	pthread_mutex_unlock(&mutex_q);
	pthread_exit(NULL);
}

/* Schedualler Thread Initial Function  */
void *
sched_thread_func(void * arg){

	Node dQNode;
	
	pthread_mutex_lock(&mutex);
	while(numFlows != 0){

		pthread_cond_wait(&cond, &mutex_q);
			
		pthread_mutex_lock(&mutex_trans);	
		pthread_mutex_lock(&mutex_q);
		if(remFlows.size != 0){
			/*  deQ next flow and signal to transmit  */
			sort();
			dQNode = deQ();
			transID = dQNode.data.id;
			
			pthread_cond_broadcast(&trans_cond);
			
					
		}	
		pthread_mutex_unlock(&mutex_trans);
		pthread_mutex_unlock(&mutex_q);

	}
	pthread_mutex_unlock(&mutex);
		
	pthread_exit(NULL);
	
}

/*
This function takes a pointer to an input file and a pointer to an array of flows
and fills in the array info according the input file
*/

void
printFlow(Flow *flow){
	
	printf("ID: %d\n", flow ->id);
	printf("arrival: %d\n", flow->arrival);
	printf("trans: %d\n", flow->trans);
	printf("priority: %d\n", flow->priority);
}

/* This function performs an enqueue to the queue of waiting flows */
void enQ(Flow item){

	Node *newNode;

	newNode = (Node*)malloc(sizeof(Node));
	if(newNode == NULL){
		printf("MALLOC FAIL\n");
		exit(1);
	}
	newNode->data = item;
	newNode->next=NULL;
	
	//queue is empty
	if( remFlows.size == 0 ){
		remFlows.head = newNode;
		remFlows.tail = newNode;
		remFlows.size++;
	}
	else{
		remFlows.tail->next = newNode;
		remFlows.tail = newNode;
		remFlows.size++;
		
	}
}


/* This function performs a dequeue on the queue of waiting flows.
	It returns the node removed.*/
Node deQ(){

	Node node;
	
	node = *(remFlows.head);
	if((remFlows.head->next) != NULL){
		remFlows.head = remFlows.head->next;
	}
	return node;
}

/* Print function for queue of waiting flows  */
void printQ(){
	printf("\n");
	printf("printing Q\n");
	Node *nodePtr;

	for(nodePtr = remFlows.head; nodePtr != NULL; nodePtr = nodePtr->next){
		printFlow(&(nodePtr->data));
	}

}

/* This function is used by sort to swap node's information given pointers 
	to two nodes */
void swap(Node *i, Node *j){
				Flow tempFlow;
				tempFlow = i->data;
				
				i->data = j->data;
				j->data = tempFlow;
}

/* This function sorts the queue of waiting flows
	using insertion sort.  It first sorts by priority, then
	arrival time, then transmission time.*/
void sort(){

	Node *i;
	Node *j;

	for(i = remFlows.head; i != NULL; i = i->next){
	
		for(j = i; j != NULL; j = j->next){

			//if node i priority less than node j priority, swap
			if( (i->data.priority) > (j->data.priority) ){
				//printf("swap\n");
				swap(i,j);
			}
			
			//if priorities equal, compare arrival times
			else if( (i->data.priority) == (j->data.priority) ){
				if((i->data.arrival) > (j->data.arrival) ){
					//printf("swap\n");
					swap(i,j);
				}
				//if arrival times equal, compare transmission times
				else if((i->data.arrival) == (j->data.arrival) ){
					if((i->data.trans) > (j->data.trans) ){
						swap(i,j);
					}
				}
			}
		}
	}
	
}
