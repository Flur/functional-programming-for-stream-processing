package main

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, HttpResponse}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, RunnableGraph, Sink, Source}
import akka.{Done, NotUsed}
import spray.json.DefaultJsonProtocol.{jsonFormat, jsonFormat1, jsonFormat4, lazyFormat}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}
// should be added for Unmarshal
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

case class Event(id: Int, title: String, description: String, link: String)
case class NasaResponse(title: String, description: String, link: String/*, events: List[Event]*/)

case class TestData(
                     id: Int,
                     email: String,
                     first_name: String,
                     last_name: String,
                     avatar: String,
                   )
case class Support(
//                    url: String,
                    text: String,

                  )
case class Test(
               data: TestData,
               support: Support
               )

object NasaDisasterSource {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()
  implicit val eventFormat = jsonFormat4(Event)
  implicit val nasaResponseFormat = jsonFormat3(NasaResponse)

  implicit val testDataFormat = jsonFormat5(TestData)
  implicit val supportFormat = jsonFormat1(Support)
  implicit val testFormat = jsonFormat2(Test)

  def apply(): Unit = {
    val httpRequest = HttpRequest(HttpMethods.GET, "https://eonet.sci.gsfc.nasa.gov/api/v3/events")
//    val httpRequest = HttpRequest(HttpMethods.GET, "https://reqres.in/api/users/2")

    // todo make
    // Http().singleRequest(req)
    val source: Source[(HttpRequest, Int), NotUsed] = Source.repeat((httpRequest, 1)).throttle(1, 10.seconds)

    val flow: Flow[(HttpRequest, Int), (Try[HttpResponse], Int), NotUsed] = Http().superPool[Int]()

    val sink: Sink[(Try[HttpResponse], String), Future[Done]] = Sink.foreach[(Try[HttpResponse], String)]((a: (Try[HttpResponse], String)) => println(a._1))

    source.via(flow).runForeach  {
      case (Success(resp), p) => {
        val f: Future[NasaResponse] = Unmarshal(resp.entity).to[NasaResponse]

        println(resp)

        f.onComplete {
          case Success(res) => println(res)
          case Failure(a) => println(a)
        }
      }
      case (Failure(e), p)    => println(e)
    }

//    source.via(flow)

//    source.via(flow).to(sink)
  }

}
