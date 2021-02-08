package main

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.http.scaladsl.model.ws.{Message, TextMessage}
import akka.stream.{ActorMaterializer, ClosedShape, FlowShape, Graph}
import akka.stream.javadsl.RunnableGraph
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Merge, Sink, Source, Zip, ZipWith}
import GraphDSL.Implicits._
import akka.http.scaladsl.client.RequestBuilding.WithTransformation
import com.google.api.services.calendar.model.Event

import scala.concurrent.Future
import scala.util.Try

case class EonetCategory(id: String, title: String)
case class EonetSource(id: String, url: String)
case class EonetGeometry(
                     magnitudeValue: Option[Int],
                     magnitudeUnit: Option[String],
                     date: String,
                     `type`: String,
                     coordinates: Either[List[Double], List[List[List[Double]]]],
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

case class Point(latitude: Double, longitude: Double)

case class EonetGeometryFromDB(
                              id: Int,
                              `type`: String,
                              point: Option[List[Double]],
                              polygon: Option[List[List[Double]]],
                              eventId: Int
                              )

case class EonetEventFromDB(
                             id: Int,
                             eonetEventId: String,
                             title: String,
                             description: Option[String],
                             link: Option[String],
                             closed: Option[Boolean],
                             geometry: Option[List[EonetGeometryFromDB]],
                           )

case class EonetResponse(title: String, description: String, link: String, events: List[EonetEvent])

object Main extends App {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  val sourceOfGoogleEvent = runEventsThruPostgre()
  val sourceOfEonetEvent = runDisastersThruPostgre()

  val googleEventGr = getGraphForNewGoogleCalendarEvent()
  val eonetEventGr = getGraphForNewEonetEvent()

  val mainSource: Source[(List[EonetEventFromDB], NominatimResponse, Event), NotUsed] = Source.combine(
    Source.fromGraph(sourceOfGoogleEvent.via(googleEventGr)),
    Source.fromGraph(sourceOfEonetEvent.via(eonetEventGr))
  )(Merge(_))

  val compareCoordinatesFlow = CompareCoordinatesService.getFlow()
  val sourceForWebSocketServer = getSourceForWebSocketServer(mainSource.via(compareCoordinatesFlow))

//  sourceOfGoogleEvent.runForeach(println("end of data", _))
//  sourceOfEonetEvent.via(eonetEventGr).runForeach(println("end of data", _))

  HttpServer.start(sourceForWebSocketServer)

  def getSourceForWebSocketServer(
                                   source: Source[(List[(EonetEventFromDB, List[NominatimGeoJSON])], Event), NotUsed]
                                 ): Source[Message, NotUsed] =
    Source.fromGraph(source)
      .map((t: (List[(EonetEventFromDB, List[NominatimGeoJSON])], Event)) => {
      val (listOfEvents, googleEvent) = t

      val location = googleEvent.getLocation
      val listOfEonetEvents = listOfEvents.map(t => {
        val (ee, nominatimGeoJSONs) = t
        val locationOfEvents = nominatimGeoJSONs.map(n => n.properties.display_name).mkString(" or ")

        s"""  Event description: ${ee.description}
           |  Event Location: ${locationOfEvents}
           |  Event Link: ${ee.link.get}""".stripMargin
      }).mkString(", ")

      TextMessage(
        s"""Near your google calendar event ${googleEvent.getHtmlLink}
           |that located on $location there are some disasters in radius of 30km
           |${listOfEonetEvents}""".stripMargin)
    })

  def getGraphForNewGoogleCalendarEvent(): Graph[
    FlowShape[Event, (List[EonetEventFromDB], NominatimResponse, Event)], NotUsed
  ] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      // prepare graph elements
      val broadcast = builder.add(Broadcast[Event](3))
      val zip = builder.add(
        ZipWith[List[EonetEventFromDB], NominatimResponse, Event, (List[EonetEventFromDB], NominatimResponse, Event)] {
          (eonetEventsList, nominatimResponse, googleEvent) => (eonetEventsList, nominatimResponse, googleEvent)
        })

      val postgre = Flow[Event].mapAsync(1) { event => PostgresService.getActiveEonetEvent() }
      val searchEventLocation = NominatimService.apply


      // connect the graph
      broadcast.out(0) ~> postgre ~> zip.in0
      broadcast.out(1) ~> searchEventLocation ~> zip.in1
      broadcast.out(2) ~>  zip.in2

      // expose ports
      FlowShape(broadcast.in, zip.out)
    })
  }

  def getGraphForNewEonetEvent(): Graph[
    FlowShape[EonetEvent, (List[EonetEventFromDB], NominatimResponse, Event)], NotUsed
  ] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
      // prepare graph elements
      val broadcastEonetEvent = builder.add(Broadcast[EonetEvent](2))


      val zip = builder.add(
        ZipWith[EonetEvent, Event, NominatimResponse, (List[EonetEventFromDB], NominatimResponse, Event)] {
          (eonetEvent, googleEvent, nominatimResponse) => {
            (List(EonetEventService.eonetEvetToEonetEventFromDB(eonetEvent)), nominatimResponse, googleEvent)
          }
        })

      val broadcastGoogleEvent = builder.add(Broadcast[Event](2))

      val postgres: Flow[EonetEvent, Event, NotUsed] = Flow[EonetEvent]
        .mapAsync(150)((e: EonetEvent) => PostgresService.getGoogleCalendarEvents())
        .mapConcat(identity)

      val searchEventLocation = NominatimService.apply

      broadcastEonetEvent.out(0) ~> zip.in0
      broadcastEonetEvent.out(1) ~> postgres ~> broadcastGoogleEvent.in
                                                broadcastGoogleEvent.out(1) ~> zip.in1
                                                broadcastGoogleEvent.out(0) ~> searchEventLocation ~> zip.in2

      // expose ports
      FlowShape(broadcastEonetEvent.in, zip.out)
    })
  }

  def runDisastersThruPostgre(): Source[EonetEvent, NotUsed] = {
    val disasterSource: Source[EonetEvent, NotUsed] = EonetDisasterSource.apply
    val postgreService: Flow[EonetEvent, Option[EonetEvent], NotUsed] = PostgresService.getEonetEventFlow

    disasterSource
      .via(postgreService)
      .filter {
      case Some(a) => true
      case None => false
    }
      .map(_.get)
  }

  def runEventsThruPostgre(): Source[Event, NotUsed] = {
    val disasterSource: Source[Event, NotUsed] = GoogleCalendarService.getEventSource
    val postgreService: Flow[Event, Option[Event], NotUsed] = PostgresService.getGoogleCalendarEventFlow

    disasterSource
      .via(postgreService)
      .filter {
        case Some(a) => true
        case None => false
      }
      .map(_.get)
  }
}
