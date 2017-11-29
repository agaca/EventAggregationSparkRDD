package org.dummy


import org.apache.spark.rdd.RDD

import scala.collection.mutable



class EventAggregation() extends Serializable {

  /**
    * Metodo auxiliar. Desglosa los eventos que se han producido en un dia en una lista de pares.
    * Cada par (K,V) representa la ocurrencia de un evento, con K=evento y V=duracion evento.
    *
    * Ejemplo linea
    * -  AAAAABAAAAABAAAAABAAAAAB
    *
    * Proporciona el siguiente resultado:
    * -  List((A,5), (B,1), (A,5), (B,1), (A,5), (B,1), (A,5), (B,1))
    *
    * @param eventsLine String con una linea del fichero que se va a procesar,
    *                   en este caso se pasa la lista de eventos producidos en un dia
    *                   sin fecha incluida: AAAAABAAAAABAAAAABAAAAAB.
    * @return List[(String, Long)] Lista de pares que representan la ocurrencia de cada evento y duracion de la misma
    *
    *
    */
  def ocsPerLine(eventsLine: String): List[(String, Long)] =
    if (eventsLine.isEmpty) List()
    else {
      val oc = eventsLine.head
      (oc.toString, eventsLine.takeWhile(c => c == oc).length.toLong) :: ocsPerLine(eventsLine.dropWhile(c => c == oc))
    }

  /**
    * Este método cálcula el número de ocurrencias de cada tipo de evento-
    * Por ejemplo, en el caso de unas cadenas como las siguientes:
    * -  1:AAAAABAAAAABAAAAABAAAAAB
    * -  2:BBAAAAAAAAAABBAAAAAAAAAA
    * En este caso el resultado debería ser Map{ "A" -> 6 , "B" -> 6 }
    *
    * @param rdd RDD con las lineas del fichero que se va a procesar,
    *            el formato de cada linea es 1:AAAAABAAAAABAAAAABAAAAAB.
    * @return Un diccionario con cada tipo de evento y el numero de ocurrencias
    *         asociadas al mismo.
    */
  def numberOfOccurrences(rdd: RDD[String]): Map[String, Long] =
    rdd.map(line => line.dropWhile(c => !c.isLetter))
      .flatMap(eventsLine => ocsPerLine(eventsLine))
      .aggregateByKey(0.toLong)((acc, curr) => acc + 1, (l, r) => l + r)
      .collect
      .toMap

  /**
    * Este método calcula la duracción total de cada tipo de evento.
    * Por ejemplo, en el caso de unas cadenas como las siguientes:
    * -  1:AAAAABAAAAABAAAAABAAAAAB
    * -  2:BBAAAAAAAAAABBAAAAAAAAAA
    * En este caso el resultado debería ser Map{ "A" -> 40 , "B" -> 8 }
    *
    * @param rdd RDD con las lineas del fichero que se va a procesar,
    *            el formato de cada linea es 1:AAAAABAAAAABAAAAABAAAAAB.
    * @return La duracción total a cada tipo de evento.
    */
  def calculateTotalDuration(rdd: RDD[String]): Map[String, Long] =
    rdd.map(line => line.dropWhile(c => !c.isLetter))
      .flatMap(eventsLine => ocsPerLine(eventsLine))
      .reduceByKey(_ + _)
      .collect()
      .toMap

  /**
    * Metodo auxiliar. Desglosa los eventos que se han producido en un dia en una lista de pares.
    * Cada par (k,V) representa una transicion entre eventos, con k=(evento1,evento2) y V=1 (se marca una transicion con
    * el valor 1 cada vez que se detecta, independientemente de que se haya producido anteriormente)
    *
    * Ejemplo linea
    * -  AAAAABAAAAABAAAAABAAAAAB
    *
    * Proporciona el siguiente resultado:
    * -  List(((A,B),1), ((B,A),1), ((A,B),1), ((B,A),1), ((A,B),1), ((B,A),1), ((A,B),1))
    *
    * @param eventsLine String con una linea del fichero que se va a procesar,
    *                   en este caso se pasa la lista de eventos producidos en un dia
    *                   sin fecha incluida: AAAAABAAAAABAAAAABAAAAAB.
    * @return List[((String, String), Long)] Lista con transiciones entre eventos con marca = 1
    *
    *
    */
  def lineTransitions(eventsLine: String): List[((String, String), Long)] =
    if (eventsLine.isEmpty) List()
    else {
      val nextTrans = eventsLine.dropWhile(c => c == eventsLine.head)
      if (!nextTrans.isEmpty) ((eventsLine.head.toString, nextTrans.head.toString), 1.toLong) :: lineTransitions(nextTrans)
      else List()
    }

  /**
    * Este método calcula el numero total de transiciones que se han
    * producido en un dataset.
    * Por ejemplo, en el caso de unas cadenas como las siguientes:
    * -  1:AAAAABAAAAABAAAAABAAAAAB
    * -  2:BBAAAAAAAAAABBAAAAAAAAAA
    * El resultado debería ser
    * Map{ ("A","B") -> 5 , ("B","A") -> 5 }
    *
    * @param rdd RDD con las lineas del fichero que se va a procesar,
    *            el formato de cada linea es 1:AAAAABAAAAABAAAAABAAAAAB.
    * @return Un diccionario con cada tipo de transiciones que han aparecido
    *         en el dataset y el numero de ocurrencias.: Map[(String, String), Long]
    */
  def calculateNumberOfTransitions(rdd: RDD[String]): Map[(String, String), Long] =
    rdd.map(line => line.dropWhile(c => !c.isLetter))
      .flatMap(eventsLine => lineTransitions(eventsLine))
      .aggregateByKey(0.toLong)((acc, curr) => acc + curr, (l, r) => l + r)
      .collect
      .toMap


  /**
    * Este método calcula el dia para cada uno de los tipos donde el evento ha
    * tenido una ocurrencia con la duracción máxima.
    * Por ejemplo, en el caso de unas cadenas como las siguientes:
    * -  1:AAAAABAAAAABAAAAABAAAAAB
    * -  2:BBAAAAAAAAAABBAAAAAAAAAA
    * -  3:BBBBBBBBAAAABBAAAAAAAAAA
    * En este caso el resultado debería ser Map{ "A" -> Day2 , "B" -> Day3 }
    *
    * @param rdd RDD con las lineas del fichero que se va a procesar,
    *            el formato de cada linea es 1:AAAAABAAAAABAAAAABAAAAAB.
    * @return Un diccionario con cada tipo y el día donde aparece la ocurrencia
    *         más durarera. : Map[String, Long] =
    */
  def calculateDayWithMaxDuration(rdd: RDD[String]): Map[String, Long] =
    rdd.flatMap(line => {
      val events = line.dropWhile(c => !c.isLetter)
      val date = line.takeWhile(d => d.isDigit).toLong
      val zero: Map[String, Long] = Map().withDefaultValue(0.toLong)
      ocsPerLine(events).foldLeft(zero)((acc, curr) => {
        if (curr._2 > acc.apply(curr._1)) acc.updated(curr._1, curr._2)
        else acc
      })
        .toList
        .map(occ => (occ._1, (occ._2, date)))
    })
      .reduceByKey((acc, curr) => if (curr._1 > acc._1) curr else acc)
      .map(occDate => (occDate._1, occDate._2._2))
      .collect
      .toMap
}
