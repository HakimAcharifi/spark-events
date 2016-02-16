# spark-events
POC pour valider le mode d'exécution des stages de spark-streaming

## Comment ça marche ?

au debut du test, deux types de messages sont injectés dans kafka :

 > DBUpdateEvent(id: String, value: String)
   
    l'id et la value de cet event sont arbitraires et une seule instance de cet event est emise vers kafka
    cet message provoque l'enregistrement de sa valeur dans cassandra avec comme clef de partition son id
 
 > TxEvent(id: String, value: String)
    
    l'id de ce message est arbitraire mais sa valeur est la concatenation de l'id et de la valeur 
    de l'unique event DBUpdateEvent emis dans kafka

le schema d'execution est le suivant :

1. Premier cas : traitement sans shuffle (voir NoMapWaitEventProcessing)
 
  kafka ---> map ---> map ---> filter ---> map ---> reduce ---> foreach
  
  dans ce cas de figure, certains TXEvent ne veront pas les effets induits par l'event DBUpdateEvent
  dans cassandra avant de finir leur course dans le foreach
  
2. Deuxieme cas : traitement avec shuffle (voir MapWaitEventProcessing)

  kafka ---> map ---> map ---> filter ---> groupBy ---> flatMap ---> map ---> reduce ---> foreach
  
  dans ce scenario un shuffle intervient au niveau du groupBy. Ce shuffle contraint spark a executer et finir
  toutes les transformations du stage precedent avant de passer au stage suivant qui commence apres le groupBy.
  la consequence de ce model est que tous les messages a la sortie du filter veront tous les effets induits
  par chacun d'entre eux dans le stage suivant. Donc tous les TXEvent devront voir les modifications effectuees
  par l'event DBUpdateEvent dans cassandra et ce quelque soit le temps que cette operation necessite.
   
   

