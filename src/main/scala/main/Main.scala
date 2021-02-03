package main

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import scala.concurrent.Future
import scala.util.Try

case class EonetCategory(id: String, title: String)
case class EonetSource(id: String, url: String)
case class EonetGeometry(
                     magnitudeValue: Option[Int],
                     magnitudeUnit: Option[String],
                     date: String,
                     `type`: String,
                        // blew my head off
                     coordinates: Either[List[Float], List[List[List[Float]]]],
                    )

case class EonetEvent(
                       id: String,
                       title: String,
                       description: Option[String],
                       link: String,
                       closed: Option[Boolean],
                       categories: List[EonetCategory],
                       sources: List[EonetSource],
                       geometry: List[EonetGeometry],
                     )

// 1. insert categories -> select categories -> id categories

case class EonetResponse(title: String, description: String, link: String, events: List[EonetEvent])

//case class EonetSourceDB(eonet_id: Int) extends EonetSource()

case class GoogleEvent()

object Main extends App {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  PostgresService.apply

  val disasterSource: Source[EonetEvent, NotUsed] = EonetDisasterSource.apply
  val postgreService: Flow[EonetEvent, Option[EonetEvent], NotUsed] = PostgresService.getFlow

  disasterSource.via(postgreService).runForeach(println(_))
//  disasterSource.runForeach(println(_))

//  PostgresService.apply()

//  val sinkEonetEventsSink = PostgresService.getDisastersSink()

//  disasterSource.runWith(sinkEonetEventsSink)

//  disasterSource.map((response) => response.events).runWith(sinkEonetEventsSink)
//  val b = disasterSource.runWith(Sink.foreach(println(_)))

//  val messageHandler: Flow[Message, Message, Any] = Flow.apply

//  HttpServer.start(messageHandler)
}
