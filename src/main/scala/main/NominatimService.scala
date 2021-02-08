package main

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.{HttpMethods, HttpRequest, Uri}
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import com.google.api.services.calendar.model.Event
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.DurationInt
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import scala.concurrent.Future

case class NominatimGeometry(`type`: String, coordinates: Either[List[Double], List[List[Double]]])
// todo add props
case class NominatimGeoJSONProperty(display_name: String)
case class NominatimGeoJSON(`type`: String, bbox: List[Double], geometry: NominatimGeometry, properties: NominatimGeoJSONProperty)
case class NominatimResponse(`type`: String, licence: String, features: List[NominatimGeoJSON])

object NominatimService {
  implicit val system = ActorSystem()
  implicit val dispatcher = system.dispatcher
  implicit val materializer = ActorMaterializer()

  implicit val geometryFormat = jsonFormat2(NominatimGeometry)
  implicit val geoJSONPropertyFormat = jsonFormat1(NominatimGeoJSONProperty)
  implicit val GeoJSONFormat = jsonFormat4(NominatimGeoJSON)
  implicit val responseFormat = jsonFormat3(NominatimResponse)

  def apply: Flow[Event, NominatimResponse, NotUsed] = {
    Flow[Event]
      // limitation see here https://operations.osmfoundation.org/policies/nominatim/
      .throttle(1, 1.second)
      .map { event => {

          HttpRequest(
            HttpMethods.GET,
            Uri(s"https://nominatim.openstreetmap.org/search")
              .withQuery(Query("q" -> event.getLocation, "format" -> "geojson", "dedupe" -> "1")))
        }
      }
      .mapAsync(150) {
        req => {
          Http().singleRequest(req).flatMap(r => Unmarshal(r.entity).to[NominatimResponse])
        }
      }
  }

  def getNominatimGeometryCoordinates(nominatimGeoJSON: NominatimGeoJSON): List[List[Double]] = {
    nominatimGeoJSON.geometry.coordinates match {
      case Left(a) => List(a)
      case Right(b) => b
    }
  }
}
