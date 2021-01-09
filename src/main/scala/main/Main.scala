package main

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future
import scala.util.Try


object Main extends App {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val httpRequest = HttpRequest(HttpMethods.GET, "https://eonet.sci.gsfc.nasa.gov/api/v3/events")

  val source: Source[(HttpRequest, String), NotUsed] = Source.single((httpRequest, "test"))

  val flow: Flow[(HttpRequest, String), (Try[HttpResponse], String), NotUsed] = Http().superPool[String]()

  val sink: Sink[(Try[HttpResponse], String), Future[Done]] = Sink.foreach[(Try[HttpResponse], String)]((a: (Try[HttpResponse], String)) => println(a._1))

  val mat = source.via(flow).runWith(sink)
}
