package main

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.NotUsed
import spray.json.DefaultJsonProtocol.jsonFormat4
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.DurationInt
// should be added for Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

object EonetDisasterSource {
  // make one actor for all flow?
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  implicit val eonetSourceFormat = jsonFormat2(EonetSource)
  implicit val categoryFormat = jsonFormat2(EonetCategory)
  implicit val eonetGeometryFormat = jsonFormat5(EonetGeometry)
  implicit val eventFormat = jsonFormat8(EonetEvent)
  implicit val nasaResponseFormat = jsonFormat4(EonetResponse)

  def apply: Source[EonetEvent, NotUsed] = {
    val httpRequest = HttpRequest(HttpMethods.GET, "https://eonet.sci.gsfc.nasa.gov/api/v3/events")

    // todo add errors handling for HttpRequests, maybe change parallelism value
    // todo could be used tick instead of repeat and throttle
    Source
      .repeat(httpRequest)
      .throttle(1, 30.seconds)
      .mapAsync(1) { req => Http().singleRequest(req).flatMap(r => Unmarshal(r.entity).to[EonetResponse]) }
      .mapConcat { eonetResponse => eonetResponse.events }
  }
}
